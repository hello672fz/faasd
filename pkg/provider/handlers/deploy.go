package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/containerd/containerd"
	"github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/containers"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/oci"
	gocni "github.com/containerd/go-cni"
	"github.com/distribution/reference"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/openfaas/faas-provider/types"
	cninetwork "github.com/openfaas/faasd/pkg/cninetwork"
	"github.com/openfaas/faasd/pkg/service"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
)

const annotationLabelPrefix = "com.openfaas.annotations."

//var images = make(map[string]string)

// MakeDeployHandler returns a handler to deploy a function
func MakeDeployHandler(client *containerd.Client, cni gocni.CNI, secretMountPath string, alwaysPull bool) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {

		if r.Body == nil {
			http.Error(w, "expected a body", http.StatusBadRequest)
			return
		}

		defer r.Body.Close()

		body, _ := io.ReadAll(r.Body)

		req := types.FunctionDeployment{}
		err := json.Unmarshal(body, &req)
		if err != nil {
			log.Printf("[Deploy] - error parsing input: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
		log.Printf("[Deploy] - error parsing input: %s", err)
		namespace := getRequestNamespace(req.Namespace)

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

		namespaceSecretMountPath := getNamespaceSecretMountPath(secretMountPath, namespace)
		err = validateSecrets(namespaceSecretMountPath, req.Secrets)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		name := req.Service
		ctx := namespaces.WithNamespace(context.Background(), namespace)

		if err := preDeploy(client, 1); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("[Deploy] error deploying %s, error: %s\n", name, err)
			return
		}

		if err := deploy(ctx, req, client, cni, namespaceSecretMountPath, alwaysPull); err != nil {
			log.Printf("[Deploy] error deploying %s, error: %s\n", name, err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
}

// prepull is an optimization which means an image can be pulled before a deployment
// request, since a deployment request first deletes the active function before
// trying to deploy a new one.
func prepull(ctx context.Context, req types.FunctionDeployment, client *containerd.Client, alwaysPull bool) (containerd.Image, error) {
	start := time.Now()
	//TODO:use exsiting cnt
	r, err := reference.ParseNormalizedNamed(req.Image)
	if err != nil {
		return nil, err
	}

	imgRef := reference.TagNameOnly(r).String()
	//images[req.Service] = imgRef

	snapshotter := ""
	if val, ok := os.LookupEnv("snapshotter"); ok {
		snapshotter = val
	}

	//image, err := service.PrepareImage(ctx, client, imgRef, snapshotter, alwaysPull)
	image, err := service.PrepareImage(ctx, client, imgRef, snapshotter, false)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to pull image %s", imgRef)
	}

	size, _ := image.Size(ctx)
	log.Printf("Image for: %s size: %d, took: %fs\n", image.Name(), size, time.Since(start).Seconds())

	return image, nil
}

func deploy(ctx context.Context, req types.FunctionDeployment, client *containerd.Client, cni gocni.CNI, secretMountPath string, alwaysPull bool) error {

	snapshotter := ""
	if val, ok := os.LookupEnv("snapshotter"); ok {
		snapshotter = val
	}

	image, err := prepull(ctx, req, client, alwaysPull)

	if err != nil {
		return err
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

	name := req.Service

	labels, err := buildLabels(&req)
	if err != nil {
		return fmt.Errorf("unable to apply labels to container: %s, error: %w", name, err)
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
	//create containers.Container from the input-containers.Container
	container, err := client.NewContainer(
		ctx,
		name,
		containerd.WithImage(image),
		containerd.WithSnapshotter(snapshotter),
		containerd.WithNewSnapshot(name+"-snapshot", image),
		containerd.WithNewSpec(oci.WithImageConfig(image),
			oci.WithHostname(name),
			oci.WithCapabilities([]string{"CAP_NET_RAW"}),
			oci.WithMounts(mounts),
			oci.WithEnv(envs),
			withMemory(memory)),
		containerd.WithContainerLabels(labels),
	)
	//container, err := client.NewContainer(
	//	ctx,
	//	name,
	//	containerd.WithNewSnapshot(name+"-snapshot23", image),
	//	containerd.WithNewSpec(oci.WithImageConfig(image)),
	//)

	if err != nil {
		return fmt.Errorf("unable to create container: %s, error: %w", name, err)
	}

	//return createTask(ctx, container, cni)
	return createTaskWithCheckpoint(ctx, container, cni)

}

// countFunctions returns the number of functions deployed along with a map with a count
// in each namespace
func countFunctions(client *containerd.Client) (int64, int64, error) {
	count := int64(0)
	namespaceCount := int64(0)

	namespaces := ListNamespaces(client)

	for _, namespace := range namespaces {
		fns, err := ListFunctions(client, namespace)
		if err != nil {
			return 0, 0, err
		}
		namespaceCount++
		count += int64(len(fns))
	}

	return count, namespaceCount, nil
}

func buildLabels(request *types.FunctionDeployment) (map[string]string, error) {
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

func createTask(ctx context.Context, container containerd.Container, cni gocni.CNI) error {

	name := container.ID()

	task, taskErr := container.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil))

	if taskErr != nil {
		return fmt.Errorf("unable to start task: %s, error: %w", name, taskErr)
	}

	log.Printf("Container ID: %s\tTask ID %s:\tTask PID: %d\t\n", name, task.ID(), task.Pid())

	labels := map[string]string{}
	_, err := cninetwork.CreateCNINetwork(ctx, cni, task, labels)

	if err != nil {
		return err
	}

	ip, err := cninetwork.GetIPAddress(name, task.Pid())
	if err != nil {
		return err
	}

	log.Printf("%s has IP: %s.\n", name, ip)

	if _, err := task.Wait(ctx); err != nil {
		return errors.Wrapf(err, "Unable to wait for task to start: %s", name)
	}

	if startErr := task.Start(ctx); startErr != nil {
		return errors.Wrapf(startErr, "Unable to start task: %s", name)
	}
	return nil
}

//
//func createTaskWithCheckpoint(ctx context.Context, container containerd.Container, cni gocni.CNI) error {
//
//	name := container.ID()
//
//	// create image path store criu image files
//	imagePath := "/tmp/checkpoint"
//	filePath := filepath.Join(imagePath, container.ID()+"-ckpt")
//
//	//task, taskErr := container.NewTask(ctx, cio.NullIO, containerd.WithRestoreImagePath(filePath))
//	task, taskErr := container.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil), containerd.WithRestoreImagePath(filePath))
//	//task, taskErr := container.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil))
//	//// 获取容器的状态，包括 Snapshotkey
//	//r, err := container.get(ctx)
//	//if err != nil {
//	//	return fmt.Errorf("failed to get container state: %s, error: %w", name, err)
//	//}
//	//
//	//// 输出 r.Snapshotter
//	//log.Printf("Container ID: %s\tSnapshotkey: %s\n", name, r.Snapshotkey)
//
//	if taskErr != nil {
//		fmt.Errorf("unable to start task: %s, error: %w", name, taskErr)
//	}
//
//	log.Printf("Container ID: %s\tTask ID %s:\tTask PID: %d\t\n", name, task.ID(), task.Pid())
//
//	//ip, err := cninetwork.GetIPAddress(name, task.Pid())
//	//if err != nil {
//	//	fmt.Errorf("unable to GetIPAddress")
//	//	return err
//	//}
//	//
//	//log.Printf("%s has IP: %s.\n", name, ip)
//
//	_, err := task.Wait(ctx)
//
//	if err != nil {
//		fmt.Errorf("Unable to wait for task to start: %s", name)
//		return err
//	}
//
//	if startErr := task.Start(ctx); startErr != nil {
//		fmt.Errorf("Unable to start task: %s", name)
//		return err
//	}
//
//	labels := map[string]string{}
//	_, err = cninetwork.CreateCNINetwork(ctx, cni, task, labels)
//
//	if err != nil {
//		fmt.Errorf("unable to CreateCNINetwork")
//		return err
//	}
//
//	//// create image path store criu image files
//	//imagePath = "/tmp/checkpoint"
//	//filePath = filepath.Join(imagePath, container.ID()+"-ckpt")
//	//
//	//// Remove the directory and its contents if it exists
//	//err = os.RemoveAll(filePath)
//	//if err != nil {
//	//	fmt.Errorf("failed to delete directory %s: %w", filePath, err)
//	//	return err
//	//}
//	//
//	//// Create a new empty directory
//	//err = os.Mkdir(filePath, 0755) // 0755 sets read, write, and execute permissions
//	//if err != nil {
//	//	fmt.Errorf("failed to create directory %s: %w", filePath, err)
//	//	return err
//	//}
//	//
//	//// checkpoint task
//	//if _, err := task.Checkpoint(ctx, containerd.WithCheckpointImagePath(filePath)); err != nil {
//	//	return err
//	//}
//
//	return nil
//}

func createTaskWithCheckpoint(ctx context.Context, container containerd.Container, cni gocni.CNI) error {

	name := container.ID()

	task, taskErr := container.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil))
	//// 获取容器的状态，包括 Snapshotkey
	//r, err := container.get(ctx)
	//if err != nil {
	//	return fmt.Errorf("failed to get container state: %s, error: %w", name, err)
	//}
	//
	//// 输出 r.Snapshotter
	//log.Printf("Container ID: %s\tSnapshotkey: %s\n", name, r.Snapshotkey)

	if taskErr != nil {
		fmt.Errorf("unable to start task: %s, error: %w", name, taskErr)
		return taskErr
	}

	log.Printf("Container ID: %s\tTask ID %s:\tTask PID: %d\t\n", name, task.ID(), task.Pid())

	labels := map[string]string{}
	_, err := cninetwork.CreateCNINetwork(ctx, cni, task, labels)

	if err != nil {
		return err
	}

	ip, err := cninetwork.GetIPAddress(name, task.Pid())
	if err != nil {
		return err
	}

	log.Printf("%s has IP: %s.\n", name, ip)

	_, err = task.Wait(ctx)

	if err != nil {
		errors.Wrapf(err, "Unable to wait for task to start: %s", name)
		return err
	}

	if startErr := task.Start(ctx); startErr != nil {
		errors.Wrapf(startErr, "Unable to start task: %s", name)
		return err
	}

	// create image path store criu image files
	imagePath := "/tmp/checkpoint"
	_, err = os.Stat(imagePath)
	if os.IsNotExist(err) {
		fmt.Printf("Directory does not exist: %s\n, now creating", imagePath)
		err = os.Mkdir(imagePath, 0755)
		if err != nil {
			fmt.Println("Error creating directory:", err)
			return err
		}
	}

	filePath := filepath.Join(imagePath, container.ID()+"-ckpt")

	// Remove the directory and its contents if it exists
	err = os.RemoveAll(filePath)
	if err != nil {
		fmt.Errorf("failed to delete directory %s: %w", filePath, err)
		return err
	}

	// Create a new empty directory
	err = os.Mkdir(filePath, 0755) // 0755 sets read, write, and execute permissions
	if err != nil {
		fmt.Errorf("failed to create directory %s: %w", filePath, err)
		return err
	}

	// checkpoint task
	if _, err := task.Checkpoint(ctx, containerd.WithCheckpointImagePath(filePath)); err != nil {
		return err
	}

	//if err := task.Kill(ctx, syscall.SIGKILL); err != nil {
	//	return nil, errors.Wrapf(err, "Unable to kill for task to start: %s", name)
	//}
	//<-statusC
	//
	//if _, err := task.Delete(ctx); err != nil {
	//	return nil, errors.Wrapf(err, "Unable to Delete for task to start: %s", name)
	//}

	//task, taskErr = container.NewTask(ctx, cio.NewCreator(cio.WithStdio), containerd.WithRestoreImagePath(filePath))
	////task, taskErr = container.NewTask(ctx, cio.BinaryIO("/usr/local/bin/faasd", nil))
	//// TODO: test
	//if taskErr != nil {
	//	return fmt.Errorf("unable to restore task: %s, error: %w", name, taskErr)
	//}
	//
	//log.Printf("Container ID: %s\tTask ID %s:\tTask PID: %d\t\n", name, task.ID(), task.Pid())
	//
	//labels = map[string]string{}
	//_, err = cninetwork.CreateCNINetwork(ctx, cni, task, labels)
	//
	//if err != nil {
	//	return err
	//}
	//
	//ip, err = cninetwork.GetIPAddress(name, task.Pid())
	//if err != nil {
	//	return err
	//}
	//
	//log.Printf("%s has IP: %s.\n", name, ip)
	//
	//_, err = task.Wait(ctx)
	//
	//if err != nil {
	//	return errors.Wrapf(err, "Unable to wait for task to start: %s", name)
	//}
	//
	//if startErr := task.Start(ctx); startErr != nil {
	//	return errors.Wrapf(startErr, "Unable to start task: %s", name)
	//}

	//checkpoint, err := task.Checkpoint(ctx)
	//err := client.Push(ctx, "myregistry/checkpoints/redis:master", checkpoint)   192.168.1.176:5000/checkpoint/helloworldlfz:cr-1

	//return task, nil
	return nil
}

func prepareEnv(envProcess string, reqEnvVars map[string]string) []string {
	envs := []string{}
	fprocessFound := false
	fprocess := "fprocess=" + envProcess
	if len(envProcess) > 0 {
		fprocessFound = true
	}

	for k, v := range reqEnvVars {
		if k == "fprocess" {
			fprocessFound = true
			fprocess = v
		} else {
			envs = append(envs, k+"="+v)
		}
	}
	if fprocessFound {
		envs = append(envs, fprocess)
	}
	return envs
}

// getOSMounts provides a mount for os-specific files such
// as the hosts file and resolv.conf
func getOSMounts() []specs.Mount {
	// Prior to hosts_dir env-var, this value was set to
	// os.Getwd()
	hostsDir := "/var/lib/faasd"
	if v, ok := os.LookupEnv("hosts_dir"); ok && len(v) > 0 {
		hostsDir = v
	}

	mounts := []specs.Mount{}
	mounts = append(mounts, specs.Mount{
		Destination: "/etc/resolv.conf",
		Type:        "bind",
		Source:      path.Join(hostsDir, "resolv.conf"),
		Options:     []string{"rbind", "ro"},
	})

	mounts = append(mounts, specs.Mount{
		Destination: "/etc/hosts",
		Type:        "bind",
		Source:      path.Join(hostsDir, "hosts"),
		Options:     []string{"rbind", "ro"},
	})
	return mounts
}

func validateSecrets(secretMountPath string, secrets []string) error {
	for _, secret := range secrets {
		if _, err := os.Stat(path.Join(secretMountPath, secret)); err != nil {
			return fmt.Errorf("unable to find secret: %s", secret)
		}
	}
	return nil
}

func withMemory(mem *specs.LinuxMemory) oci.SpecOpts {
	return func(ctx context.Context, _ oci.Client, c *containers.Container, s *oci.Spec) error {
		if mem != nil {
			if s.Linux == nil {
				s.Linux = &specs.Linux{}
			}
			if s.Linux.Resources == nil {
				s.Linux.Resources = &specs.LinuxResources{}
			}
			if s.Linux.Resources.Memory == nil {
				s.Linux.Resources.Memory = &specs.LinuxMemory{}
			}
			s.Linux.Resources.Memory.Limit = mem.Limit
		}
		return nil
	}
}

func preDeploy(client *containerd.Client, additional int64) error {
	count, countNs, err := countFunctions(client)
	log.Printf("Function count: %d, Namespace count: %d\n", count, countNs)

	if err != nil {
		return err
	} else if count+additional > faasdMaxFunctions {
		return fmt.Errorf("the OpenFaaS CE EULA allows %d/%d function(s), upgrade to faasd Pro to continue", faasdMaxFunctions, count+additional)
	} else if countNs > faasdMaxNs {
		return fmt.Errorf("the OpenFaaS CE EULA allows %d/%d namespace(s), upgrade to faasd Pro to continue", faasdMaxNs, countNs)
	}
	return nil
}

//// WithCheckpointImagePath sets image path for checkpoint option
//func WithCheckpointImagePath(path string) CheckpointTaskOpts {
//	return func(r *CheckpointTaskInfo) error {
//		if r.Options == nil {
//			r.Options = &options.CheckpointOptions{}
//		}
//		opts, ok := r.Options.(*options.CheckpointOptions)
//		if !ok {
//			return errors.New("invalid runtime v2 checkpoint options format")
//		}
//		opts.ImagePath = path
//		return nil
//	}
//}
