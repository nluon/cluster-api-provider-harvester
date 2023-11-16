/*
Copyright 2022.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	harvesterv1beta1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubevirtv1 "kubevirt.io/api/core/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/v1beta1"
	"sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/cluster-api/util/annotations"
	"sigs.k8s.io/cluster-api/util/patch"
	"sigs.k8s.io/cluster-api/util/predicates"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	infrav1 "github.com/rancher-sandbox/cluster-api-provider-harvester/api/v1alpha1"
	harvclient "github.com/rancher-sandbox/cluster-api-provider-harvester/pkg/clientset/versioned"
	locutil "github.com/rancher-sandbox/cluster-api-provider-harvester/util"
)

// HarvesterMachineReconciler reconciles a HarvesterMachine object
type HarvesterMachineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	vmAnnotationPVC        = "harvesterhci.io/volumeClaimTemplates"
	vmAnnotationNetworkIps = "networks.harvesterhci.io/ips"
	hvAnnotationDiskNames  = "harvesterhci.io/diskNames"
	hvAnnotationSSH        = "harvesterhci.io/sshNames"
	hvAnnotationImageID    = "harvesterhci.io/imageId"
	listImagesSelector     = ".spec.displayName="
)

//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachines,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachines/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvestermachines/finalizers,verbs=update
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=harvesterclusters,verbs=get;list
//+kubebuilder:rbac:groups=infrastructure.cluster.x-k8s.io,resources=clusters;machines,verbs=get;list;watch

func (r *HarvesterMachineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, rerr error) {
	logger := log.FromContext(ctx).WithValues("harvestermachine", req.NamespacedName)
	ctx = ctrl.LoggerInto(ctx, logger)

	logger.Info("Reconciling HarvesterMachine ...")

	hvMachine := &infrav1.HarvesterMachine{}
	if err := r.Get(ctx, req.NamespacedName, hvMachine); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Error(err, "harvestermachine not found")
			return ctrl.Result{}, nil
		}

		logger.Error(err, "Error happened when getting harvestermachine")
		return ctrl.Result{}, err
	}

	// Initialize the patch helper
	patchHelper, err := patch.NewHelper(hvMachine, r.Client)
	if err != nil {
		return ctrl.Result{}, err
	}
	// Always attempt to Patch the HarvesterMachine object and status after each reconciliation.
	defer func() {
		if err := patchHelper.Patch(ctx,
			hvMachine,
		//conditions.WithOwnedConditions( []clusterv1.ConditionType{ clusterv1.ReadyCondition}),
		); err != nil {
			logger.Error(err, "failed to patch DockerMachine")
			if rerr == nil {
				rerr = err
			}
		}
	}()

	if !hvMachine.DeletionTimestamp.IsZero() {
		return r.ReconcileDelete(ctx, hvMachine)
	} else {
		return r.ReconcileNormal(ctx, hvMachine)
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *HarvesterMachineReconciler) SetupWithManager(ctx context.Context, mgr ctrl.Manager) error {
	clusterToHarvesterMachine, err := util.ClusterToObjectsMapper(mgr.GetClient(), &infrav1.HarvesterMachineList{}, mgr.GetScheme())
	if err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&infrav1.HarvesterMachine{}).
		Watches(
			&source.Kind{Type: &clusterv1.Machine{}},
			handler.EnqueueRequestsFromMapFunc(util.MachineToInfrastructureMapFunc(infrav1.GroupVersion.WithKind("HarvesterMachine"))),
			builder.WithPredicates(predicates.ResourceNotPaused(ctrl.LoggerFrom(ctx))),
		).
		Watches(
			&source.Kind{Type: &clusterv1.Cluster{}},
			handler.EnqueueRequestsFromMapFunc(clusterToHarvesterMachine),
			builder.WithPredicates(predicates.ClusterUnpaused(ctrl.LoggerFrom(ctx))),
		).
		Complete(r)
}

func (r *HarvesterMachineReconciler) ReconcileNormal(ctx context.Context, hvMachine *infrav1.HarvesterMachine) (res reconcile.Result, rerr error) {
	logger := log.FromContext(ctx)

	ownerMachine, err := util.GetOwnerMachine(ctx, r.Client, hvMachine.ObjectMeta)
	if err != nil {
		logger.Error(err, "unable to get owner machine")
		return ctrl.Result{}, err
	}
	if ownerMachine == nil {
		logger.Info("Waiting for Machine Controller to set OwnerRef on HarvesterMachine")
		return ctrl.Result{}, nil
	}

	ownerCluster, err := util.GetClusterFromMetadata(ctx, r.Client, ownerMachine.ObjectMeta)
	if err != nil {
		logger.Info("HarvesterMachine owner Machine is missing cluster label or cluster does not exist")
		return ctrl.Result{}, err
	}
	if ownerCluster == nil {
		logger.Info(fmt.Sprintf("Please associate this machine with a cluster using the label %s: <name of cluster>", clusterv1.ClusterNameLabel))
		return ctrl.Result{}, nil
	}

	logger = logger.WithValues("HarvesterMachine", ownerCluster.Namespace+"/"+ownerCluster.Name)
	ctx = ctrl.LoggerInto(ctx, logger)

	// Return early if the object or Cluster is paused.
	if annotations.IsPaused(ownerCluster, hvMachine) {
		logger.Info("Reconciliation is paused for this object")
		return ctrl.Result{}, nil
	}

	// Add finalizer first if not exist to avoid the race condition between init and delete
	if !controllerutil.ContainsFinalizer(hvMachine, infrav1.MachineFinalizer) {
		controllerutil.AddFinalizer(hvMachine, infrav1.MachineFinalizer)
		return ctrl.Result{}, nil
	}

	// Return early if the ownerCluster has infrastructureReady = false
	if !ownerCluster.Status.InfrastructureReady {
		logger.Info("Waiting for Infrastructure to be ready ... ")
		return ctrl.Result{}, nil
	}

	// Return early if no userdata secret is referenced in ownerMachine
	if ownerMachine.Spec.Bootstrap.DataSecretName == nil {
		logger.Info("Waiting for Machine's Userdata to be set ... ")
		return ctrl.Result{}, nil
	}

	logger = logger.WithValues("machine", ownerMachine, "cluster", ownerCluster)
	ctx = ctrl.LoggerInto(ctx, logger)

	var hvCluster *infrav1.HarvesterCluster

	hvClusterKey := types.NamespacedName{
		Namespace: ownerCluster.Spec.InfrastructureRef.Namespace,
		Name:      ownerCluster.Spec.InfrastructureRef.Name,
	}

	err = r.Get(ctx, hvClusterKey, hvCluster)
	if err != nil {
		logger.Error(err, "unable to find corresponding harvestercluster to harvestermachine")
		return ctrl.Result{}, err
	}

	hvSecret, err := locutil.GetSecretFromHarvesterCluster(ctx, hvCluster, r.Client)
	if err != nil {
		logger.Error(err, "unable to get Datasource secret")
		return ctrl.Result{}, err
	}

	hvClient, err := locutil.GetHarvesterClientFromSecret(hvSecret)
	if err != nil {
		logger.Error(err, "unable to create Harvester client from Datasource secret", hvClient)
	}

	_, err = createVMFromHarvesterMachine(hvMachine, hvClient)
	if err != nil {
		logger.Error(err, "unable to create VM from HarvesterMachine information")
	}

	//TODO: Set the `spec.ProviderID`
	//TODO: Set status.ready = true
	//TODO: Set status.addresses with IP addresses of VM

	return ctrl.Result{}, nil
}

func createVMFromHarvesterMachine(hvMachine *infrav1.HarvesterMachine, hvClient *harvclient.Clientset) (*kubevirtv1.VirtualMachine, error) {
	var err error

	vmLabels := map[string]string{
		"harvesterhci.io/creator": "harvester",
	}
	vmiLabels := vmLabels

	vmName := hvMachine.Name

	vmiLabels["harvesterhci.io/vmName"] = vmName
	vmiLabels["harvesterhci.io/vmNamePrefix"] = vmName
	diskRandomID := locutil.RandomID()
	pvcName := vmName + "-disk-0-" + diskRandomID

	hasVMIMageName := func(volume infrav1.Volume) bool { return volume.ImageName != "" }

	// Supposing that the imageName field in HarvesterMachine.Spec.Volumes has the format "<NAMESPACE>/<NAME>",
	// we use the following to get vmImageNS and vmImageName
	imageVolumes := locutil.Filter[infrav1.Volume](hvMachine.Spec.Volumes, hasVMIMageName)
	vmImage, err := getImageFromHarvesterMachine(imageVolumes, hvClient)
	if err != nil {
		return nil, errors.Wrap(err, "unable to find VM image reference in HarvesterMachine")
	}
	pvcAnnotation, err := buildPVCAnnotationFromImageID(&imageVolumes[0], pvcName, hvMachine.Spec.TargetNamespace, vmImage)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to generate PVC annotation on VM")
	}

	vmTemplate, err := buildVMTemplate(hvClient, pvcName, vmiLabels, *hvMachine)
	if err != nil {
		return &kubevirtv1.VirtualMachine{}, errors.Wrap(err, "unable to build VM definition")
	}

	if vmTemplate.ObjectMeta.Labels == nil {
		vmTemplate.ObjectMeta.Labels = make(map[string]string)
	}

	vmTemplate.ObjectMeta.Labels["harvesterhci.io/vmNamePrefix"] = vmName
	vmTemplate.Spec.Affinity = &v1.Affinity{
		PodAntiAffinity: &v1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
				{
					Weight: int32(1),
					PodAffinityTerm: v1.PodAffinityTerm{
						TopologyKey: "kubernetes.io/hostname",
						LabelSelector: &metav1.LabelSelector{
							MatchLabels: map[string]string{
								"harvesterhci.io/vmNamePrefix": vmName,
							},
						},
					},
				},
			},
		},
	}

	ubuntuVM := &kubevirtv1.VirtualMachine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      vmName,
			Namespace: hvMachine.Spec.TargetNamespace,
			Annotations: map[string]string{

				vmAnnotationPVC:        pvcAnnotation,
				vmAnnotationNetworkIps: "[]",
			},
			Labels: vmLabels,
		},
		Spec: kubevirtv1.VirtualMachineSpec{
			Running: locutil.NewTrue(),

			Template: vmTemplate,
		},
	}

	hvCreatedMachine, err := hvClient.KubevirtV1().VirtualMachines(hvMachine.Spec.TargetNamespace).Create(context.TODO(), ubuntuVM, metav1.CreateOptions{})

	if err != nil {
		return hvCreatedMachine, err
	}

	return hvCreatedMachine, nil
}

func buildPVCAnnotationFromImageID(imageVolume *infrav1.Volume, pvcName string, pvcNamespace string, vmImage *harvesterv1beta1.VirtualMachineImage) (string, error) {

	block := v1.PersistentVolumeBlock
	scName := "longhorn"
	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: pvcNamespace,
			Annotations: map[string]string{
				hvAnnotationImageID: vmImage.Namespace + "/" + vmImage.Name,
			},
		},
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{
				v1.ReadWriteMany,
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					"storage": *imageVolume.VolumeSize,
				},
			},
			VolumeMode:       &block,
			StorageClassName: &scName,
		},
	}

	pvcJsonString, err := json.Marshal(pvc)
	if err != nil {
		return "", err
	}

	return strconv.Quote(string(pvcJsonString)), nil
}

func getImageFromHarvesterMachine(imageVolumes []infrav1.Volume, hvClient *harvclient.Clientset) (image *harvesterv1beta1.VirtualMachineImage, err error) {

	vmImageNamespacedName := imageVolumes[0].ImageName

	vmImageNameParts := strings.Split(vmImageNamespacedName, "/")
	if len(vmImageNameParts) != 2 {
		return &harvesterv1beta1.VirtualMachineImage{}, fmt.Errorf("ImageName is HarvesterMachine is Malformed, expecting <NAMESPACE>/<NAME> format")
	}

	foundImages, err := hvClient.HarvesterhciV1beta1().VirtualMachineImages(vmImageNameParts[0]).List(context.TODO(), metav1.ListOptions{
		FieldSelector: listImagesSelector + vmImageNameParts[1],
	})

	if err != nil {
		return &harvesterv1beta1.VirtualMachineImage{}, err
	}
	if len(foundImages.Items) == 0 {
		return &harvesterv1beta1.VirtualMachineImage{}, fmt.Errorf("impossible to find referenced VM image from imageName field in HarvesterMachine")
	}

	// returning namespaced name of the vm image
	return &foundImages.Items[0], nil
}

// buildVMTemplate creates a *kubevirtv1.VirtualMachineInstanceTemplateSpec from the CLI Flags and some computed values
func buildVMTemplate(hvClient *harvclient.Clientset,
	pvcName string, vmiLabels map[string]string, hvMachine infrav1.HarvesterMachine) (vmTemplate *kubevirtv1.VirtualMachineInstanceTemplateSpec, err error) {

	var err1 error
	cloudInitUserData, err1 := getCloudInitData(hvMachine, "user")
	vmTemplate = nil
	if err1 != nil {
		err = fmt.Errorf("error during getting cloud init user data from Harvester: %w", err1)
		return
	}

	var sshKey *harvesterv1beta1.KeyPair

	keyName := hvMachine.Spec.SSHKeyPair
	sshKey, err1 = hvClient.HarvesterhciV1beta1().KeyPairs(hvMachine.Spec.TargetNamespace).Get(context.TODO(), keyName, metav1.GetOptions{})
	if err1 != nil {
		err = fmt.Errorf("error during getting keypair from Harvester: %w", err1)
		return
	}
	logrus.Debugf("SSH Key Name %s given does exist!", hvMachine.Spec.SSHKeyPair)

	if sshKey == nil || sshKey == (&harvesterv1beta1.KeyPair{}) {
		err = fmt.Errorf("no keypair could be defined")
		return
	}

	cloudInitSSHSection := "\nssh_authorized_keys:\n  - " + sshKey.Spec.PublicKey + "\n"

	if err1 != nil {
		err = fmt.Errorf("error during getting cloud-init for networking: %w", err1)
		return
	}

	vmTemplate = &kubevirtv1.VirtualMachineInstanceTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				hvAnnotationDiskNames: "[\"" + pvcName + "\"]",
				hvAnnotationSSH:       "[\"" + pvcName + "\"]",
			},
			Labels: vmiLabels,
		},
		Spec: kubevirtv1.VirtualMachineInstanceSpec{
			Hostname: hvMachine.Name,
			Networks: []kubevirtv1.Network{

				{
					Name: "nic-1",

					NetworkSource: kubevirtv1.NetworkSource{
						Multus: &kubevirtv1.MultusNetwork{
							NetworkName: "vlan1",
						},
					},
				},
			},
			Volumes: []kubevirtv1.Volume{
				{
					Name: "disk-0",
					VolumeSource: kubevirtv1.VolumeSource{
						PersistentVolumeClaim: &kubevirtv1.PersistentVolumeClaimVolumeSource{
							PersistentVolumeClaimVolumeSource: v1.PersistentVolumeClaimVolumeSource{
								ClaimName: pvcName,
							},
						},
					},
				},
				{
					Name: "cloudinitdisk",
					VolumeSource: kubevirtv1.VolumeSource{
						CloudInitNoCloud: &kubevirtv1.CloudInitNoCloudSource{
							UserData: cloudInitUserData + cloudInitSSHSection,
						},
					},
				},
			},
			Domain: kubevirtv1.DomainSpec{
				CPU: &kubevirtv1.CPU{
					Cores:   uint32(hvMachine.Spec.CPU),
					Sockets: uint32(hvMachine.Spec.CPU),
					Threads: uint32(hvMachine.Spec.CPU),
				},
				Devices: kubevirtv1.Devices{
					Inputs: []kubevirtv1.Input{
						{
							Bus:  "usb",
							Type: "tablet",
							Name: "tablet",
						},
					},
					Interfaces: []kubevirtv1.Interface{
						{
							Name:                   "nic-1",
							Model:                  "virtio",
							InterfaceBindingMethod: kubevirtv1.DefaultBridgeNetworkInterface().InterfaceBindingMethod,
						},
					},
					Disks: []kubevirtv1.Disk{
						{
							Name: "disk-0",
							DiskDevice: kubevirtv1.DiskDevice{
								Disk: &kubevirtv1.DiskTarget{
									Bus: "virtio",
								},
							},
						},
						{
							Name: "cloudinitdisk",
							DiskDevice: kubevirtv1.DiskDevice{
								Disk: &kubevirtv1.DiskTarget{
									Bus: "virtio",
								},
							},
						},
					},
				},
				Resources: kubevirtv1.ResourceRequirements{
					Requests: v1.ResourceList{
						"memory": resource.MustParse(hvMachine.Spec.Memory),
					},
				},
			},
			Affinity: &v1.Affinity{
				PodAntiAffinity: &v1.PodAntiAffinity{
					PreferredDuringSchedulingIgnoredDuringExecution: []v1.WeightedPodAffinityTerm{
						{
							Weight: int32(1),
							PodAffinityTerm: v1.PodAffinityTerm{
								TopologyKey: "kubernetes.io/hostname",
								LabelSelector: &metav1.LabelSelector{
									MatchLabels: map[string]string{
										"harvesterhci.io/vmNamePrefix": hvMachine.Name,
									},
								},
							},
						},
					},
				},
			},
		},
	}
	return
}

func getCloudInitData(hvMachine infrav1.HarvesterMachine, s string) (string, error) {

	// TODO: Implement
	return "", nil
}

func (r *HarvesterMachineReconciler) ReconcileDelete(ctx context.Context, hvMachine *infrav1.HarvesterMachine) (res ctrl.Result, rerr error) {

	return ctrl.Result{}, nil
}
