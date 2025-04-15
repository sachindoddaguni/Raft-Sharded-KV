package shardctrler

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOldRequest  = "ErrOldRequest"
)

type Err string

type JoinArgs struct {
	Servers       map[int][]string // new GID -> servers mappings
	ClientId      int64
	RequestNumber int32
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs          []int
	ClientId      int64
	RequestNumber int32
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard         int
	GID           int
	ClientId      int64
	RequestNumber int32
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num           int // desired config number
	ClientId      int64
	RequestNumber int32
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
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
