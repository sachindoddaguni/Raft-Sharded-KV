package shardkv

import (
	"crypto/rand"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"6.824/shardctrler"

	"context"
	"fmt"
	"log"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  = "OK"
	ErrNoKey            = "ErrNoKey"
	ErrWrongGroup       = "ErrWrongGroup"
	ErrWrongLeader      = "ErrWrongLeader"
	ErrServerNotUpdated = "ErrServerNotUpdated"
	ErrNoGroupAvailable = "ErrNoGroupAvailable"
	KeyLocked           = "KeyLocked"
	TransactionFailed   = "TransactionFailed"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	ClientId      int64
	RequestNumber int32
	ConfigNumber  int
	TxID          string
}

type PutAppendReply struct {
	Err Err
}

type TxOp struct {
	Ops           []Op
	ClientId      int64
	RequestNumber int32
	Delay         int
}

type GetArgs struct {
	Key           string
	ClientId      int64
	RequestNumber int32
	ConfigNumber  int
}

type GetReply struct {
	Err   Err
	Value string
}

func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func termIndexToString(term int, index int) string {
	return strconv.Itoa(term) + "." + strconv.Itoa(index)
}

type LockArgs struct {
	Keys          []string
	TxID          string
	ClientId      int64
	RequestNumber int32
}

type LockReply struct {
	Err string
}

type UnlockArgs struct {
	Keys          []string
	TxID          string // The transaction that wants to release the locks.
	ClientId      int64
	RequestNumber int32
}

type UnlockReply struct {
	Err string
}

// createLogContainer creates and starts a container from the given image and containerName,
// binding the container's port 8080 to a host port. If hostPort is empty (""), Docker
// is instructed to assign a random available port. The function returns the container ID
// and the actual host port that was assigned.
func createContainer(image, containerName string) (string, string, error) {
	// Create a Docker client.
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return "", "", fmt.Errorf("failed to create Docker client: %v", err)
	}
	ctx := context.Background()

	// Prepare the container configuration: expose port 8080.
	config := &container.Config{
		Image: image,
		ExposedPorts: nat.PortSet{
			"8080/tcp": struct{}{},
		},
		Tty: false,
	}

	hostPort := "0"

	// Set up host configuration with port binding.
	hostConfig := &container.HostConfig{
		PortBindings: nat.PortMap{
			"8080/tcp": []nat.PortBinding{
				{
					HostIP:   "0.0.0.0",
					HostPort: hostPort, // Docker assigns a random port if "0" is passed.
				},
			},
		},
	}

	// Create the container.
	resp, err := cli.ContainerCreate(ctx, config, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", "", fmt.Errorf("failed to create container: %v", err)
	}

	// Start the container.
	if err := cli.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return "", "", fmt.Errorf("failed to start container: %v", err)
	}

	// Allow a short time for Docker to set up the port mapping.
	time.Sleep(500 * time.Millisecond)

	// Inspect the container to get the assigned host port.
	inspect, err := cli.ContainerInspect(ctx, resp.ID)
	if err != nil {
		return resp.ID, "", fmt.Errorf("failed to inspect container: %v", err)
	}
	// Look up the assigned host port for container port "8080/tcp".
	bindings, ok := inspect.NetworkSettings.Ports["8080/tcp"]
	if !ok || len(bindings) == 0 {
		return resp.ID, "", fmt.Errorf("failed to obtain port mapping for 8080/tcp")
	}
	assignedHostPort := bindings[0].HostPort

	log.Printf("Container %s started with ID: %s, binding container port 8080 to host port %s", containerName, resp.ID, assignedHostPort)
	return resp.ID, assignedHostPort, nil
}

func logToServer(port string, message string) error {
	// Construct the URL where the log server is listening.
	url := "http://localhost:" + port + "/log"

	// Create a new POST request with the log message as the body.
	req, err := http.NewRequest("POST", url, strings.NewReader(message))
	if err != nil {
		return fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "text/plain")

	// Create an HTTP client with a timeout.
	client := &http.Client{Timeout: 3 * time.Second}

	// Send the request.
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %v", err)
	}
	defer resp.Body.Close()

	// Check the response status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response status: %v", resp.Status)
	}

	return nil
}

func deleteContainersWithPrefix() error {
	prefixes := []string{"ctrler", "server"}
	// Create a Docker client from environment.
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return fmt.Errorf("failed to create docker client: %v", err)
	}
	ctx := context.Background()

	// List all containers (including stopped containers).
	containers, err := cli.ContainerList(ctx, types.ContainerListOptions{All: true})
	if err != nil {
		return fmt.Errorf("failed to list containers: %v", err)
	}

	for _, container := range containers {
		// Each container can have multiple names.
		for _, name := range container.Names {
			// Docker container names are usually prefixed with '/'
			trimmedName := strings.TrimPrefix(name, "/")
			for _, prefix := range prefixes {
				if strings.HasPrefix(trimmedName, prefix) {
					log.Printf("Deleting container: %s (ID: %s)", trimmedName, container.ID)
					// Remove the container. Force removal (Force: true) to stop it if it's running.
					err := cli.ContainerRemove(ctx, container.ID, types.ContainerRemoveOptions{
						Force: true,
					})
					if err != nil {
						log.Printf("Error removing container %s: %v", trimmedName, err)
					}
					break // container name matches one prefix, move on to next container
				}
			}
		}
	}

	return nil
}

func killContainer(containerID string, signal string) error {
	// 1. Initialize client
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
	if err != nil {
		return fmt.Errorf("unable to create Docker client: %w", err)
	}
	ctx := context.Background()

	// 2. Kill (signal) the container
	if err := cli.ContainerKill(ctx, containerID, signal); err != nil {
		return fmt.Errorf("failed to kill container %q: %w", containerID, err)
	}

	// 3. Remove the container metadata (force just in case it’s still running)
	removeOpts := types.ContainerRemoveOptions{
		Force:         true,  // will send SIGKILL if somehow still running
		RemoveVolumes: false, // set to true if you also want to drop anonymous volumes
	}
	if err := cli.ContainerRemove(ctx, containerID, removeOpts); err != nil {
		return fmt.Errorf("failed to remove container %q: %w", containerID, err)
	}

	return nil
}
