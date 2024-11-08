package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/containerd/containerd/oci"
	"github.com/distribution/reference"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/openfaas/faasd/pkg/cninetwork"
	"github.com/openfaas/faasd/pkg/service"
	"github.com/pkg/errors"
	"io"
	"k8s.io/apimachinery/pkg/api/resource"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/namespaces"
	//"github.com/containerd/containerd/v2/client"
	gocni "github.com/containerd/go-cni"
	"github.com/openfaas/faas-provider/types"
	"github.com/openfaas/faasd/pkg"
)

type ScaleServiceRequest2 struct {
	ServiceName string                   `json:"serviceName"`
	Replicas    uint64                   `json:"replicas"`
	Namespace   string                   `json:"namespace,omitempty"`
	Image       string                   `json:"image"`
	EnvProcess  string                   `json:"envProcess,omitempty"`
	EnvVars     map[string]string        `json:"envVars,omitempty"`
	Secrets     []string                 `json:"secrets,omitempty"`
	Limits      *types.FunctionResources `json:"limits,omitempty"`
	Labels      *map[string]string       `json:"labels,omitempty"`
	Annotations *map[string]string       `json:"annotations,omitempty"`
	Checkpoint  uint64                   `json:"checkpoint,omitempty"`
}

func MakeReplicaUpdateHandler(client *containerd.Client, secretMountPath string, cni gocni.CNI) func(w http.ResponseWriter, r *http.Request) {

	return func(w http.ResponseWriter, r *http.Request) {

		log.Printf("[Scale Start] \n")

		if r.Body == nil {
			http.Error(w, "expected a body", http.StatusBadRequest)
			return
		}

		defer r.Body.Close()

		body, _ := io.ReadAll(r.Body)

		req := ScaleServiceRequest2{}
		if err := json.Unmarshal(body, &req); err != nil {
			log.Printf("[Scale] error parsing input: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		namespace := req.Namespace
		if namespace == "" {
			namespace = pkg.DefaultFunctionNamespace
		}

		// Check if namespace exists, and it has the openfaas label
		valid, err := validNamespace(client.NamespaceService(), namespace)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if !valid {
			http.Error(w, "namespace not valid", http.StatusBadRequest)
			return
		}

		name := req.ServiceName
		//if _, err := GetFunction(client, name, namespace); err != nil {
		//	msg := fmt.Sprintf("function: %s.%s not found", name, namespace)
		//	log.Printf("[Scale] %s\n", msg)
		//	http.Error(w, msg, http.StatusNotFound)
		//	return
		//}

		replicas := int(req.Replicas)

		ctx := namespaces.WithNamespace(context.Background(), namespace)

		// List existing containers for the function
		existingContainers, err := listContainersByPrefix(client, ctx, name)
		if err != nil {
			log.Printf("[Scale] error listing containers: %s", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		exsitcnt, err := client.LoadContainer(ctx, name)
		if err != nil {
			log.Printf("[Scale] error loading containers: %s", err)
		}

		//todo:for test
		//err = createTaskWithCheckpoint(ctx, exsitcnt, cni)
		// create image path store criu image files
		imagePath := "/tmp/checkpoint"
		filePath := filepath.Join(imagePath, exsitcnt.ID()+"-ckpt")

		// Remove the directory and its contents if it exists
		err = os.RemoveAll(filePath)
		if err != nil {
			fmt.Errorf("failed to delete directory %s: %w", filePath, err)
			return
		}

		// Create a new empty directory
		err = os.Mkdir(filePath, 0755) // 0755 sets read, write, and execute permissions
		if err != nil {
			fmt.Errorf("failed to create directory %s: %w", filePath, err)
			return
		}
		task, taskErr := exsitcnt.Task(ctx, nil)
		if taskErr != nil {
			fmt.Errorf("unable to start task: %s, error: %w", name, taskErr)
			return
		}
		// checkpoint task
		if _, err := task.Checkpoint(ctx, containerd.WithCheckpointImagePath(filePath)); err != nil {
			return
		}
		//todo:stop test

		for i := len(existingContainers); i < replicas; i++ {
			newContainerName := fmt.Sprintf("%s-replica-%d", name, i)
			err := createNewContainer(ctx, req, client, newContainerName, name, secretMountPath)
			if err != nil {
				log.Printf("[Scale] error creating container %s: %s", newContainerName, err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		// Find the first available (idle) container
		idleContainer, err := findIdleContainer(client, ctx, name)
		if err != nil {
			log.Printf("[Scale] error finding idle container: %s", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if idleContainer != nil {
			// Handle the request using the idle container
			log.Printf("[Scale] Using idle container: %s", idleContainer.ID())
		} else {
			log.Printf("[Scale] No idle containers available")
			http.Error(w, "No idle containers available", http.StatusServiceUnavailable)
			return
		}

		var taskExists bool
		var taskStatus *containerd.Status
		//get task info related to the container
		task, taskErr = idleContainer.Task(ctx, nil)
		if taskErr != nil {
			msg := fmt.Sprintf("cannot load task for service %s, error: %s", name, taskErr)
			log.Printf("[Scale] %s\n", msg)
			taskExists = false
		} else {
			taskExists = true
			status, statusErr := task.Status(ctx)
			if statusErr != nil {
				msg := fmt.Sprintf("cannot load task status for %s, error: %s", name, statusErr)
				log.Printf("[Scale] %s\n", msg)
				http.Error(w, msg, http.StatusInternalServerError)
				return
			} else {
				taskStatus = &status
			}
		}

		createNewTask := false

		if req.Replicas == 0 {
			http.Error(w, "replicas must > 0 for faasd CE", http.StatusBadRequest)
			return
		}

		if taskExists {
			if taskStatus != nil {
				if taskStatus.Status == containerd.Paused {
					if _, err := task.Delete(ctx); err != nil {
						log.Printf("[Scale] error deleting paused task %s, error: %s\n", name, err)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
				} else if taskStatus.Status == containerd.Stopped {
					// Stopped tasks cannot be restarted, must be removed, and created again
					if _, err := task.Delete(ctx); err != nil {
						log.Printf("[Scale] error deleting stopped task %s, error: %s\n", name, err)
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					createNewTask = true
				}
			}
		} else {
			createNewTask = true
		}

		if createNewTask {
			if req.Checkpoint == 0 {
				deployErr := createTask(ctx, idleContainer, cni)
				if deployErr != nil {
					log.Printf("[Scale] error deploying %s, error: %s\n", name, deployErr)
					http.Error(w, deployErr.Error(), http.StatusBadRequest)
					return
				}
			} else {
				////TODO:checkpoint registry
				//log.Printf("scaled container's name is (%s) ", name)
				//ckpt := "192.168.1.176:5000/checkpoint/helloworldlfz:cr-1"
				//r, err := reference.ParseNormalizedNamed(ckpt)
				//if err != nil {
				//	return fmt.Errorf("unable to parse ckpt: %s, error: %w", name, err)
				//}
				//imgRef := reference.TagNameOnly(r).String()
				//checkpoint, err := client.GetImage(ctx, imgRef)
				//if err != nil {
				//	return fmt.Errorf("unable to 1111: %s, error: %w", name, err)
				//}
				//container, err := client.NewContainer(ctx, name, containerd.WithNewSnapshot("helloworld-counter-snapshot", checkpoint))
				//container, err := client.NewContainer(ctx, name, containerd.WithNewSnapshot("192.168.1.176:5000/helloworld-counter:latest ", checkpoint))
				//if err != nil {
				//	return fmt.Errorf("unable to create container: %s, error: %w", name, err)
				//} else {
				//	log.Printf("scaled container's name is (%s) ", container.ID())
				//}
				imagePath := "/tmp/checkpoint"
				filePath := filepath.Join(imagePath, name+"-ckpt")
				if files, err := os.ReadDir(filePath); err != nil || len(files) == 0 {
					log.Printf("wrong path %s\n", filePath)
				}
				task, err := idleContainer.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil), containerd.WithRestoreImagePath(filePath))
				//task, err := idleContainer.NewTask(ctx, empty(), containerd.WithRestoreImagePath(filePath))
				if err != nil {
					log.Printf("Container ID: %s\tTask ID %s:\tTask PID: %d\t\n", name, task.ID(), task.Pid())
				}

				if startErr := task.Start(ctx); startErr != nil {
					fmt.Errorf("Unable to start task: %s", name)
				}

				labels := map[string]string{}
				_, err = cninetwork.CreateCNINetwork(ctx, cni, task, labels)

				if err != nil {
					fmt.Errorf("unable to CreateCNINetwork")
				}
			}

		}
	}
}

// findIdleContainer finds the first idle container among the containers with the given prefix
func findIdleContainer(client *containerd.Client, ctx context.Context, prefix string) (containerd.Container, error) {
	containers, err := client.Containers(ctx)
	if err != nil {
		return nil, err
	}
	for _, ctr := range containers {
		if strings.HasPrefix(ctr.ID(), prefix) {
			task, err := ctr.Task(ctx, nil)
			if err != nil {
				// If task is not running or does not exist, consider the container idle
				return ctr, nil
			}
			status, err := task.Status(ctx)
			if err != nil {
				// If task status cannot be determined, skip this container
				continue
			}
			if status.Status == containerd.Created || status.Status == containerd.Stopped {
				return ctr, nil
			}
		}
	}
	return nil, nil // No idle container found
}

// listContainersByPrefix lists containers with a given prefix in their names
func listContainersByPrefix(client *containerd.Client, ctx context.Context, prefix string) ([]string, error) {
	containers := []string{}
	containersList, err := client.Containers(ctx)
	if err != nil {
		return nil, err
	}
	for _, ctr := range containersList {
		if strings.HasPrefix(ctr.ID(), prefix) {
			containers = append(containers, ctr.ID())
		}
	}
	return containers, nil
}

// createNewContainer creates a new container with the given name and CNI configuration
func createNewContainer(ctx context.Context, req ScaleServiceRequest2, client *containerd.Client, newContainerName string, name string, secretMountPath string) error {
	// Define container creation parameters
	snapshotter := ""
	if val, ok := os.LookupEnv("snapshotter"); ok {
		snapshotter = val
	}
	envs := prepareEnv(req.EnvProcess, req.EnvVars)
	mounts := getOSMounts()

	for _, secret := range req.Secrets {
		mounts = append(mounts, specs.Mount{
			Destination: path.Join("/var/openfaas/secrets", secret),
			Type:        "bind",
			Source:      path.Join(secretMountPath, secret),
			Options:     []string{"rbind", "ro"},
		})
	}
	labels, err := buildLabels1(&req)
	if err != nil {
		return fmt.Errorf("unable to apply labels to container: %s, error: %w", newContainerName, err)
	}

	var memory *specs.LinuxMemory
	if req.Limits != nil && len(req.Limits.Memory) > 0 {
		memory = &specs.LinuxMemory{}

		qty, err := resource.ParseQuantity(req.Limits.Memory)
		if err != nil {
			log.Printf("error parsing (%q) as quantity: %s", req.Limits.Memory, err.Error())
		}
		v := qty.Value()
		memory.Limit = &v
	}

	//TODO:change specific name?
	image, err := prepull1(ctx, req, client)
	if err != nil {
		return fmt.Errorf("unable to prepull images: %s, error: %w", name, err)
	}
	container, err := client.NewContainer(
		ctx,
		newContainerName,
		containerd.WithImage(image),
		containerd.WithSnapshotter(snapshotter),
		containerd.WithNewSnapshot(newContainerName+"-snapshot", image),
		containerd.WithNewSpec(oci.WithImageConfig(image),
			oci.WithHostname(newContainerName),
			oci.WithCapabilities([]string{"CAP_NET_RAW"}),
			oci.WithMounts(mounts),
			oci.WithEnv(envs),
			withMemory(memory)),
		containerd.WithContainerLabels(labels),
	)
	if err != nil {
		return fmt.Errorf("unable to create container: %s, error: %w", newContainerName, err)
	} else {
		log.Printf("scaled container's name is (%s) ", container.ID())
	}

	// Start the container and set up networking using CNI
	// Assuming createTask is a function that creates and starts the container task
	//if err := createTask(ctx, container, cni); err != nil {
	//	return fmt.Errorf("error deploying %s: %w", name, err)
	//}

	return nil
}

func prepull1(ctx context.Context, req ScaleServiceRequest2, client *containerd.Client) (containerd.Image, error) {
	start := time.Now()

	r, err := reference.ParseNormalizedNamed(req.Image)
	if err != nil {
		return nil, err
	}
	imgRef := reference.TagNameOnly(r).String()
	//imgRef := images[req.ServiceName]
	snapshotter := ""
	if val, ok := os.LookupEnv("snapshotter"); ok {
		snapshotter = val
	}

	image, err := service.PrepareImage(ctx, client, imgRef, snapshotter, true)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to pull image %s", imgRef)
	}
	//log.Printf("lfz2")

	size, _ := image.Size(ctx)
	log.Printf("Image for: %s size: %d, took: %fs\n", image.Name(), size, time.Since(start).Seconds())

	return image, nil
}

func buildLabels1(request *ScaleServiceRequest2) (map[string]string, error) {
	labels := map[string]string{}

	if request.Labels != nil {
		for k, v := range *request.Labels {
			labels[k] = v
		}
	}

	if request.Annotations != nil {
		for k, v := range *request.Annotations {
			key := fmt.Sprintf("%s%s", annotationLabelPrefix, k)
			if _, ok := labels[key]; !ok {
				labels[key] = v
			} else {
				return nil, errors.New(fmt.Sprintf("Key %s cannot be used as a label due to a conflict with annotation prefix %s", k, annotationLabelPrefix))
			}
		}
	}

	return labels, nil
}

//
//func MakeReplicaUpdateHandler(client *containerd.Client, cni gocni.CNI) func(w http.ResponseWriter, r *http.Request) {
//
//	return func(w http.ResponseWriter, r *http.Request) {
//
//		if r.Body == nil {
//			http.Error(w, "expected a body", http.StatusBadRequest)
//			return
//		}
//
//		defer r.Body.Close()
//
//		body, _ := io.ReadAll(r.Body)
//
//		req := ScaleServiceRequest2{}
//		if err := json.Unmarshal(body, &req); err != nil {
//			log.Printf("[Scale] error parsing input: %s", err)
//			http.Error(w, err.Error(), http.StatusBadRequest)
//
//			return
//		}
//
//		namespace := req.Namespace
//		if namespace == "" {
//			namespace = pkg.DefaultFunctionNamespace
//		}
//
//		// Check if namespace exists, and it has the openfaas label
//		valid, err := validNamespace(client.NamespaceService(), namespace)
//		if err != nil {
//			http.Error(w, err.Error(), http.StatusBadRequest)
//			return
//		}
//
//		if !valid {
//			http.Error(w, "namespace not valid", http.StatusBadRequest)
//			return
//		}
//
//		name := req.ServiceName
//
//		if _, err := GetFunction(client, name, namespace); err != nil {
//			msg := fmt.Sprintf("function: %s.%s not found", name, namespace)
//			log.Printf("[Scale] %s\n", msg)
//			http.Error(w, msg, http.StatusNotFound)
//			return
//		}
//
//		ctx := namespaces.WithNamespace(context.Background(), namespace)
//		//get containers.Container according the container's id/name
//		ctr, ctrErr := client.LoadContainer(ctx, name)
//		if ctrErr != nil {
//			msg := fmt.Sprintf("cannot load service %s, error: %s", name, ctrErr)
//			log.Printf("[Scale] %s\n", msg)
//			http.Error(w, msg, http.StatusNotFound)
//			return
//		}
//
//		var taskExists bool
//		var taskStatus *containerd.Status
//		//get task info related to the container
//		task, taskErr := ctr.Task(ctx, nil)
//		if taskErr != nil {
//			msg := fmt.Sprintf("cannot load task for service %s, error: %s", name, taskErr)
//			log.Printf("[Scale] %s\n", msg)
//			taskExists = false
//		} else {
//			taskExists = true
//			status, statusErr := task.Status(ctx)
//			if statusErr != nil {
//				msg := fmt.Sprintf("cannot load task status for %s, error: %s", name, statusErr)
//				log.Printf("[Scale] %s\n", msg)
//				http.Error(w, msg, http.StatusInternalServerError)
//				return
//			} else {
//				taskStatus = &status
//			}
//		}
//
//		createNewTask := false
//
//		if req.Replicas == 0 {
//			http.Error(w, "replicas must > 0 for faasd CE", http.StatusBadRequest)
//			return
//		}
//
//		if taskExists {
//			if taskStatus != nil {
//				if taskStatus.Status == containerd.Paused {
//					if _, err := task.Delete(ctx); err != nil {
//						log.Printf("[Scale] error deleting paused task %s, error: %s\n", name, err)
//						http.Error(w, err.Error(), http.StatusBadRequest)
//						return
//					}
//				} else if taskStatus.Status == containerd.Stopped {
//					// Stopped tasks cannot be restarted, must be removed, and created again
//					if _, err := task.Delete(ctx); err != nil {
//						log.Printf("[Scale] error deleting stopped task %s, error: %s\n", name, err)
//						http.Error(w, err.Error(), http.StatusBadRequest)
//						return
//					}
//					createNewTask = true
//				}
//			}
//		} else {
//			createNewTask = true
//		}
//
//		if createNewTask {
//			deployErr := createTask(ctx, ctr, cni)
//			if deployErr != nil {
//				log.Printf("[Scale] error deploying %s, error: %s\n", name, deployErr)
//				http.Error(w, deployErr.Error(), http.StatusBadRequest)
//				return
//			}
//		}
//	}
//}
