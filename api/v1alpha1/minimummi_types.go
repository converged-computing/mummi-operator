/*
Copyright 2024.

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

package v1alpha1

import (
	"fmt"
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	// Validation
	validSamplerInterpolator = []string{"ot_feedback", "naive", "ot"}
	validSaveTypes           = []string{"simple", "tardix", "dbr", "mummi"}

	// Easy access to default values set on validate
	defaultValidatorComplex   = "ras-rbdcrd-ref-CG.gro"
	defaultValidatorResources = "martini3-validator"

	defaultBrokerInterface = "rabbitmq"
	defaultBrokerQueue     = "mummi_queue"

	defaultEncoderModel     = "chonky-model"
	defaultEncoderPositions = "CG_pos_data_summary_pos_dis_C1_v1.npz"

	defaultFeedbackDatabase      = "db-feedback-sampling.npz"
	defaultFeedbackFrameDatabase = "db-feedback-frames.npz"

	defaultPathCerts      = "/opt/clones/certs"
	defaultPathsMummiRoot = "/opt/clones/mummi-ras"
	defaultPathsResources = "/opt/clones/mummi_resources"

	defaultRegistryName       = "registry"
	defaultRegistryPort int32 = 5000

	defaultSamplerFactorExtraStructures int32 = 2
	defaultSamplerInterpolator                = "ot_feedback"
	defaultSamplerIterationsMax         int32 = 1000000
	defaultSamplerKneighbors            int32 = 10
	defaultSamplerLambdaUpperBound      int32 = 1
	defaultSamplerSubSampleFraction           = "0.051"
	defaultSaveType                           = "mummi"
)

// Note from vsoch: fields are exposed that are meaningful to edit. Fields such as job type
// or description that would not make sense to edit without also editing the mummi-ras
// code are shown but commented out (and thus not available to customize)

// MiniMummiSpec defines the desired state of MiniMummi
type MiniMummiSpec struct {

	// The OCI Registry as Storage configuration
	// This is how to expect to upload artifacts (output) generated
	// This is used in the mlserver, wfmanager, and jobs
	// Registry configuration should go here
	// +optional
	Registry OrasConfig `json:"registry"`

	// Mummi software paths, root, etc.
	// +omitempty
	Paths MummiPaths `json:"paths,omitempty"`

	// Core components that run regardless of the job type
	MLServer        MLServer        `json:"mlserver,omitempty"`
	RabbitMQ        RabbitMQ        `json:"rabbitmq,omitempty"`
	WorkflowManager WorkflowManager `json:"manager,omitempty"`

	// MiniMummi Job interface, each maps to K8s job
	// +optional
	Jobs MummiJobs `json:"jobs,omitempty"`
}

// MummiJobs holds the two MiniMummi jobs we care about currently
type MummiJobs struct {

	// CGanalysis holds the cganalysis job
	//+optional
	CgAnalysis MummiJob `json:"cganalysis"`

	// CreateSim holds the createsim job
	//+optional
	CreateSim MummiJob `json:"createsim"`
}

type JobConfig struct {

	// +kubebuilder:default=1
	// +default=1
	// +optional
	BundleSize int32 `json:"bundleSize,omitempty"`

	// JobName is the name of the job (hidden as it should not change or be exposed)
	// This will always be set by the default
	// +optional
	// JobName string `json:"jobName,omitempty"`

	// JobDescription is the description of the job
	// This will always be set by the default (hidden as it should not be changed)
	// +optional
	// JobDescription string `json:"jobDescription,omitempty"`

	// Number of nodes per job
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Nodes int32 `json:"nodes,omitempty"`

	// Max active jobs (running and in queue)
	// This should be set by the individual job, only if relevant.
	// E.g., we might control the max number of createsim, anticipating
	// each will trigger a cganalysis. Leave unset to allow up to max
	// cluster size.
	// +optional
	MaxActive int32 `json:"maxActive,omitempty"`

	// Number of processes per job
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Nproc int32 `json:"nproc,omitempty"`

	// Cores per task per job
	// 6 frontier / 3 summit / 5 on lassen (vsoch: this used to be 6 default)
	// +kubebuilder:default=3
	// +default=3
	// +optional
	CoresPerTask int32 `json:"coresPerTask,omitempty"`

	// GPUs per job
	// +optional
	Gpus int32 `json:"gpus,omitempty"`

	// If this job is nested (run inside batch ob)
	// +optional
	Nested bool `json:"nested,omitempty"`

	// Walltime (in string format) for the job
	// +optional
	Walltime string `json:"walltime,omitempty"`
}

// MummiJob
type MummiJob struct {

	// JobType is the type of job
	// This will always be set by the default, and is not exposed here
	// +optional
	// JobType string `json:"jobType,omitempty"`

	// Configuration for the job
	// +optional
	Config JobConfig `json:"config,omitempty"`

	// Namespace is inherited from MiniMummi Spec
	// container image for job (createsim or cganalysis)
	// +omitempty
	Image string `json:"image,omitempty"`

	// Variables are the simname (supplied by jobTracker)
	// and output / other paths that should not be customized
	// The script for the entrypoint is also generated by the operator
}

// WorkflowManager manages the workflow
// Most (majority) of settings are in the config, I moved all under here for a better YAML UI
type WorkflowManager struct {

	// Original had a config, but most
	// Original has env: that was empty, leaving out

	// Replicas for the deployment
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Mummi nodes (maps to MUMMI_NNODES) and defaults to 6
	// This (I think) is the total number of nodes Mummi thinks it has (a max?)
	// +kubebuilder:default=6
	// +default=6
	// +optional
	Nodes int32 `json:"nodes,omitempty"`

	// Maximum nodes to allow cluster to scale to (that jobs add up to)
	// If unset, will default to Nodes above (N=6)
	// +optional
	MaxNodes int32 `json:"maxNodes,omitempty"`

	// Cores per node (deafults to 4) maps to NCORES_PER_NODED
	// +kubebuilder:default=4
	// +default=4
	// +optional
	CoresPerNode int32 `json:"coresPerNode,omitempty"`

	// Run in interactive debug mode (sleep infinity)
	// +optional
	Interactive bool `json:"interactive,omitempty"`

	// Logging configuration and options
	// +optional
	Logging Logging `json:"logging,omitempty"`
	// Workspace was also empty, read from config.yaml and left out

	// is_gc == is garbage collecting? (defaults to true)
	// +kubebuilder:default=true
	// +default=true
	// +optional
	IsGC bool `json:"isGC"`

	// Should we do patch creation? (defaults to false)
	// +optional
	DoPatchCreation bool `json:"doPatchCreation"`

	// Should we do patch selection
	// +optional
	DoPatchSelection bool `json:"doPatchSelection"`

	// Should we run the workflow? (defaults to true)
	// +kubebuilder:default=true
	// +default=true
	// +optional
	DoWorkflow bool `json:"doWorkflow"`

	// Should we schedule jobs? (defaults to true)
	// +kubebuilder:default=true
	// +default=true
	// +optional
	DoScheduleJobs bool `json:"doScheduleJobs"`

	// Setting to use oras will default to true, left out
	// Setting for scheduler defaults to kubernetes and not flux

	// wfmanager will contact ML server which should be running (defaults to true)
	// +kubebuilder:default=true
	// +default=true
	// +optional
	DoMLServer bool `json:"doMlServer"`

	// The round ID that will serve to create ml/iter{mlserver_round_id}/ to store ML generated patches
	// +optional
	MlserverRoundId int32 `json:"mlserverRoundId"`

	// Should wfmanager will contact  UCG server which should be running (note that if ML server is on, UCG cannot be used and vice versa)
	// do_ucgserver is set to 0, we can't have at same time as mlserver
	// The follow default to 0 (false) and are not exposed here (but could be)
	// do_feedback_cg2mc:   0
	// do_cgselection:      0
	// do_feedback_aa2cg:   0

	// Filesystem and I/O ---------------------------------------------------------------
	// what type of file system to use (valid values: 'simple' / 'taridx / dbr / mummi')
	// +kubebuilder:default="mummi"
	// +default="mummi"
	// +optional
	IoType string `json:"iotype,omitempty"`

	// +kubebuilder:default="mummi"
	// +default="mummi"
	// +optional
	FbType string `json:"fbtype,omitempty"`

	// Resource description -------------------------------------------------------------
	// Portion are expressed as percentages (0 = 0%, 1 = 100%). The original had 0.3 for csim and 0.7 for cgsim
	// we want to improve upon this with autoscaling, but using portions for now
	// +kubebuilder:default="0.5"
	// +default="0.5"
	// +optional
	PortionCSim string `json:"portionCSim,omitempty"`

	// +kubebuilder:default="0.5"
	// +default="0.5"
	// +optional
	PortionCG string `json:"portionCGS,omitempty"`

	// Feedback Parameters --------------------------------------------------------------
	// If true, use weight for cg to macro feedback (defaults to true)
	// maps to fbcg_do_wts
	FeedbackCGDoWeights bool `json:"feedbackCGDoWeights,omitempty"`

	// fbaa_hvr_th
	// +kubebuilder:default="0.25"
	// +default="0.25"
	// +optional
	FeedbackAAHvrThreshold string `json:"feedbackAAHvrThreshold,omitempty"`

	// fbaa_crd_th:          0.2    # 0.1975  # Suggested range 0.2-0.3
	// +kubebuilder:default="0.2"
	// +default="0.2"
	// +optional
	FeedbackCrdThreshold string `json:"feedbackCrdThreshold,omitempty"`

	// fbaa_frame_increment: 25000  #2000  #50000 -- 2000 is a test value to accelerate fb
	// +kubebuilder:default=25000
	// +default=25000
	// +optional
	FeedbackFrameIncrement int32 `json:"feedbackFrameIncrement,omitempty"`

	// Timings --------------------------------------------------------------------------
	// number of seconds per iteration for Patch Creator (nSecPerIterPC)
	// set to 30 for: once initial data consumed, try to match output of macro model
	// +kubebuilder:default=5
	// +default=5
	// +optional
	NumberSecondsPerIterPatchCreation int32 `json:"numberSecondsPerIterPatchCreation,omitempty"`

	// number of seconds per iteration for the general workflow (nSecPerIterWF)
	// 15 was commented out default - number of seconds we wait after each WF iteration
	// if a loop finished sooner, it will wait to match the frequency
	// +kubebuilder:default=15
	// +default=15
	// +optional
	NumberSecondsPerIterWorkflow int32 `json:"numberSecondsPerIterWorkflow,omitempty"`

	// number of seconds per cg selection update (nSecPerCgSelUpdate)
	// commend: 500 nodes this is ok but should be reduesed for 100 i.e. go to 60
	// +kubebuilder:default=30
	// +default=30
	// +optional
	NumberSecondsPerCgSelectionUpdate int32 `json:"numberSecondsPerCgSelectionUpdate,omitempty"`

	// nSecPerMacroFeedback
	// +kubebuilder:default=600
	// +default=600
	// +optional
	NumberSecondsPerMacroFeedback int32 `json:"numberSecondsPerMacroFeedback,omitempty"`

	// nSecPerCGFeedback
	// +kubebuilder:default=600
	// +default=600
	// +optional
	NumberSecondsPerCgFeedback int32 `json:"numberSecondsPerCgFeedback,omitempty"`

	// How much work to do per iteration  -----------------------------------------------
	// This is patches per GC
	// Commented out: # 3300    #990  # number of macro patches to be read
	// nReadPatchesPerIter
	// +kubebuilder:default=4
	// +default=4
	// +optional
	NumberReadPatchesPerIteration int32 `json:"numberReadPatchesPerIteration,omitempty"`

	// nReadCGFramesPerIter: not exposed due to comment that is not used

	// nMaxCGFramesSelectionsPerIter
	// #50  # max number of CG frames to be selected per iter
	// +kubebuilder:default=50
	// +default=50
	// +optional
	NumberMaxCgFramesSelectionsPerIteration int32 `json:"numberMaxCgFramesSelectionsPerIteration,omitempty"`

	// nMaxJobsPerIter
	// 800   #7000  #1200  #400, 200  # max number of jobs to schedule per iter (including when restoring checkpoints!)
	// +kubebuilder:default=800
	// +default=800
	// +optional
	NumberMaxJobsPerIteration int32 `json:"numberMaxJobsPerIteration,omitempty"`

	// nMaxSelectedCGFrameBuffer
	// 400   # 280  # 500nodes, 100
	// +kubebuilder:default=400
	// +default=400
	// +optional
	NumberMaxSelectedCgFrameBuffer int32 `json:"numberMaxSelectedCgFrameBuffer,omitempty"`

	// nMaxPatchesSelectionsPerIter (important for ML server)
	// max number of structures generated by ML server to be selected per iteration of the workflow
	// +kubebuilder:default=200
	// +default=200
	// +optional
	NumberMaxPatchesSelectionsPerIteration int32 `json:"numberMaxPatchesSelectionsPerIteration,omitempty"`

	// nMaxSelectedPatchBuffer (important for ML server)
	// Max number of total structure selected. This is also the max number of creatsims at any given time
	// +kubebuilder:default=5000
	// +default=5000
	// +optional
	NumberMaxSelectedPatchBuffer int32 `json:"numberMaxSelectedPatchBuffer,omitempty"`

	// container image for the workflow manager (must be provided)
	// +omitempty
	Image string `json:"image,omitempty"`

	// Image pull policy (e.g., Always, Never, etc.)
	// +kubebuilder:default="IfNotPresent"
	// +default="IfNotPresent"
	// +omitempty
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
}

type MummiPaths struct {

	// Root for all mummi assets clones. Any specific one can be over-ridden
	// +kubebuilder:default="/opt/clones/mummi-ras"
	// +default="/opt/clones/mummi-ras"
	// +optional
	MummiRoot string `json:"mummiRoot,omitempty"`

	// Root for certificates
	// +kubebuilder:default="/opt/clones/certs"
	// +default="/opt/clones/certs"
	// +optional
	Certs string `json:"certs,omitempty"`

	// Defaults to MummiRoot if not set (eliminate this if redundant)
	// +kubebuilder:default="/opt/clones/mummi_resources"
	// +default="/opt/clones/mummi_resources"
	// +optional
	MummiResources string `json:"mummiResources,omitempty"`
}

// Logging defaults logging across components. Defaults should be set elsewhere
// TODO: we can expose a more intuitive "debug" true/false function for the user across all logging types
type Logging struct {
	// 0 indicates deactivating the memory logging (logging memory is slow)
	// +optional
	MemoryUsage int32 `json:"memoryUsage,omitempty"`

	// Logging level
	// +kubebuilder:default=2
	// +default=2
	// +optional
	Level int32 `json:"level,omitempty"`

	// Logging path, will default to the root workspace
	// +optional
	Path string `json:"path,omitempty"`

	// Log to file (defaults to unset, -1)
	// +kubebuilder:default=-1
	// +default=-1
	// +optional
	ToFile int `json:"toFile,omitempty"`

	// Log to stdout (defaults to true)
	// +kubebuilder:default=-1
	// +default=-1
	// +optional
	ToStdout int `json:"toStdout,omitempty"`
}

type MLServerConfig struct {

	// Number of nodes (defaults to 1)
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Nodes int32 `json:"nodes,omitempty"`

	// OMP_NUM_THREADS (defaults to 4)
	// +kubebuilder:default=4
	// +default=4
	// +optional
	Threads int32 `json:"threads,omitempty"`

	// Logging configuration and options
	// +optional
	Logging Logging `json:"logging,omitempty"`

	// mummi is hard coded to true - there is no other
	// option currently.
	// Note that flux is disabled by default, and use_oras enabled
}

// OrasConfig holds configuration values for the registry to be deployed
// TODO add support for https and credentials, along with remote option
type OrasConfig struct {

	// Note that the host is generated based on the registry name
	// # E.g., oras push <host>/<uri>:<sample> --plain-http .
	// If no external registry is used, we use the internal one here
	// e.g., host is: registry-0.mini-mummi.default.svc.cluster.local:5000
	// +optional
	Host string `json:"host,omitempty"`

	// Name for the registry (defaults to registry)
	// +kubebuilder:default="registry"
	// +default="registry"
	// +optional
	Name string `json:"name,omitempty"`

	// Port to use to interact with the registry
	// +optional
	Port int32 `json:"port,omitempty"`

	// Assume the registry doesn't use plain http
	// +optional
	NoPlainHttp bool `json:"plainHttp,omitempty"`

	// Assume we don't need to verify
	// +optional
	TLSVerify bool `json:"TLSVerify,omitempty"`

	// Replicas for the registry deployment
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Container image
	// +kubebuilder:default="ghcr.io/oras-project/registry:latest"
	// +default="ghcr.io/oras-project/registry:latest"
	// +optional
	Image string `json:"image,omitempty"`

	// Image pull policy (e.g., Always, Never, etc.)
	// +kubebuilder:default="IfNotPresent"
	// +default="IfNotPresent"
	// +optional
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`
}

type RabbitMQ struct {

	// Note that the credentials and certificate files are generated by the operator
	// If needed we can open this up to customize
	// credentials: "rabbitmq-credentials.json"
	// certificate: "/opt/clones/certs/client_rabbitmq_certificate.pem"

	// User (these can be generated secrets if needed for more production)
	// +kubebuilder:default="dinosaur"
	// +default="dinosaur"
	// +optional
	User string `json:"user,omitempty"`

	// Pass (these can be generated secrets if needed for more production)
	// +kubebuilder:default="dinosaur"
	// +default="dinosaur"
	// +optional
	Pass string `json:"pass,omitempty"`

	// Default vhost
	// +kubebuilder:default="dinosaur_vhost"
	// +default="dinosaur_vhost"
	// +optional
	Vhost string `json:"vhost,omitempty"`

	// Broker (for example RabbitMQ) parameters
	// +optional
	Broker RabbitMQBroker `json:"broker,omitempty"`

	// container image for rabbitmq
	// +omitempty
	Image string `json:"image,omitempty"`

	// Image pull policy (e.g., Always, Never, etc.)
	// +kubebuilder:default="IfNotPresent"
	// +default="IfNotPresent"
	// +omitempty
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Replicas for the rabbit deployment
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`
}

// PlainHttp exposes the expected positive variant of the variable
func (o *OrasConfig) PlainHttp() bool {
	return !o.NoPlainHttp
}

type RabbitMQBroker struct {

	// Interface to use (defaults to rabbitmq, unlikely to change)
	// +kubebuilder:default="interface"
	// +default="interface"
	// +optional
	Interface string `json:"interface,omitempty"`

	// Name of queue to use for mummi (defaults to mummi_queue)
	// +kubebuilder:default="mummi_queue"
	// +default="mummi_queue"
	// +optional
	Queue string `json:"queue,omitempty"`
}

type MLServerEncoder struct {

	// path to mummi resources ml directory
	// Defaults to one built into container for now
	// +kubebuilder:default="/opt/clones/mummi_resources/ml"
	// +default="/opt/clones/mummi_resources/ml"
	// +optional
	Path string `json:"path,omitempty"`

	// Built into the container via an artifact
	// +kubebuilder:default="chonky-model"
	// +default="chonky-model"
	// +optional
	Model string `json:"model,omitempty"`

	// CG positions under the encoder path
	// +kubebuilder:default="CG_pos_data_summary_pos_dis_C1_v1.npz"
	// +default="CG_pos_data_summary_pos_dis_C1_v1.npz"
	// +optional
	Positions string `json:"positions,omitempty"`
}

type SamplerFeedback struct {

	// Disable feedback for the sampler
	// +optional
	Disabled bool `json:"enabled,omitempty"`

	// The feedback database used for sampling (createsims status and validation)
	// +kubebuilder:default="db-feedback-sampling.npz"
	// +default="db-feedback-sampling.npz"
	// +optional
	Database string `json:"database,omitempty"`

	// The feedback database used for sampling  (feedback frames from cganalysis)
	// +kubebuilder:default="db-feedback-frames.npz"
	// +default="db-feedback-fames.npz"
	// +optional
	FrameDatabase string `json:"frameDatabase,omitempty"`
}

// Enabled is the reverse of disabled!
func (sf *SamplerFeedback) Enabled() bool {
	return !sf.Disabled
}

type MLServerSampler struct {

	// Type of sampler used. Can be "naive", "ot" or "ot_feedback"
	// +kubebuilder:default="ot_feedback"
	// +default="ot_feedback"
	// +optional
	Interpolator string `json:"interpolator,omitempty"`

	// This is currently not included
	// Pickle file of pre-computed interpolator (will override most settings here if chosen)
	// pre_computed: ""

	// Output path defaults to mummi_root / ml
	// +optional
	Outpath string `json:"outpath,omitempty"`

	// Feedback
	// +optional
	Feedback SamplerFeedback `json:"feedback,omitempty"`

	// This factor defines the number of extra structures we will generate
	// Example: if =2, we will select <= 2 * nMaxSelectedPatchBuffer structures
	// It is useful when many createsims are failing and we need to generate more

	// Feedback extra strucutres
	// +kubebuilder:default=2
	// +default=2
	// +optional
	FactorExtraStructures int32 `json:"factorExtraStructures,omitempty"`

	// Number of neighbors (used to calculate the k nearest neighbors)
	// +kubebuilder:default=10
	// +default=10
	// +optional
	Kneighbors int32 `json:"kneighbors,omitempty"`

	// Define lower bound for paramter lambda that will define how far from training data we are sampling
	// +optional
	LambdaLowerbound int32 `json:"lambdaLowerbound,omitempty"`

	// Define upper bound for parameter lambda
	// +kubebuilder:default=1
	// +default=1
	// +optional
	LambdaUpperbound int32 `json:"lambdaUpperbound,omitempty"`

	// Define the number of iterations used by the optimal transport package
	// +kubebuilder:default=1000000
	// +default=1000000
	// +optional
	NumberIterationsMax int32 `json:"numberIterationsMax,omitempty"`

	// This parameter defines the fraction of A and B that will actually be used (0.1 = 10%)
	// This was 0.3 before, needs to be > 0.5 and less than 1 (original was 1.0)
	// +kubebuilder:default="0.051"
	// +default="0.051"
	// +optional
	SubSampleFraction string `json:"subSampleFraction,omitempty"`
}

type MLServer struct {

	// Run in interactive debug mode (sleep infinity)
	// +optional
	Interactive bool `json:"interactive,omitempty"`

	// Replicas for the mlserver deployment
	// Each runs on one node
	// +kubebuilder:default=1
	// +default=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Config is the MLServer configuration
	// +optional
	Config MLServerConfig `json:"config"`

	// The Workspace for the MLServer is hard coded (does not need to change)
	// The RabbitMQ message server defined in the top level is used by
	// the MLServer

	// The MLServer AutoEncoder
	// Auto-encoder (ML model) parameters
	// +optional
	Encoder MLServerEncoder `json:"encoder,omitempty"`

	// The Sampler for the MLServer
	// +optional
	Sampler MLServerSampler `json:"sampler,omitempty"`

	// We don't expose the generator or validator paths
	// +optional
	Validator MLServerValidator `json:"validator,omitempty"`

	// Image pull policy (e.g., Always, Never, etc.)
	// +kubebuilder:default="IfNotPresent"
	// +default="IfNotPresent"
	// +omitempty
	ImagePullPolicy string `json:"imagePullPolicy,omitempty"`

	// Namespace is inherited from MiniMummi Spec
	// container image for MLServer (should be loaded into cluster)
	// +omitempty
	Image string `json:"image,omitempty"`
}

type MLServerValidator struct {

	// do not perform healing on the validated structures
	NoHealing bool `json:"noHealing,omitempty"`

	// Do not remove all the temporary files generated during validations
	NoCleanup bool `json:"noCleanup,omitempty"`

	// Name of the folder in the mummi_resources to pull out
	// These are for Campaign 1
	// +kubebuilder:default="martini3-validator"
	// +default="martini3-validator"
	// +optional
	Resources string `json:"resources,omitempty"`

	// +kubebuilder:default="ras-rbdcrd-ref-CG.gro"
	// +default="ras-rbdcrd-ref-CG.gro"
	// +optional
	Complex string `json:"complex,omitempty"`
}

// Expose expected variables in the positive. They are set to "No" above because that is default (false)
func (v *MLServerValidator) Healing() bool {
	return !v.NoHealing
}
func (v *MLServerValidator) Cleanup() bool {
	return !v.NoCleanup
}

// MiniMummiStatus defines the observed state of MiniMummi
type MiniMummiStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// MiniMummi is the Schema for the minimummis API
type MiniMummi struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   MiniMummiSpec   `json:"spec,omitempty"`
	Status MiniMummiStatus `json:"status,omitempty"`
}

// HasInClusterRegistry determines if we have a custom registry set
func (m *MiniMummi) HasInClusterRegistry() bool {
	return m.Spec.Registry.Host == ""
}

// RegistryHost returns an in- or external- registry host
func (m *MiniMummi) RegistryHost() string {

	// We have an external registry defined, return it
	if !m.HasInClusterRegistry() {

		// Most production registries won't require a port
		if m.Spec.Registry.Port == 0 {
			return m.Spec.Registry.Host
		}
		return fmt.Sprintf("%s:%d", m.Spec.Registry.Host, m.Spec.Registry.Port)
	}

	// registry-0.mini-mummi.default.svc.cluster.local:5000
	return fmt.Sprintf(
		"registry-0.%s.%s.svc.cluster.local:%d",
		m.Name, m.Namespace, m.Spec.Registry.Port,
	)
}

// The selector is how different objects (e.g,. deployment are added to the headless service)
func (m *MiniMummi) Selector() map[string]string {
	return map[string]string{"app": m.Name}
}

// RabbitHost returns the rabbitmq hsot
func (m *MiniMummi) RabbitHost() string {

	// rabbitmq.mini-mummi.default.svc.cluster.local
	return fmt.Sprintf("rabbitmq.%s.%s.svc.cluster.local", m.Name, m.Namespace)
}

// RabbitmQ secret name for reference across objects
func (m *MiniMummi) RabbitSecretName() string {
	return fmt.Sprintf("%s-rabbit-secrets", m.Name)
}

// MLServer Name
func (m *MiniMummi) MLServerName() string {
	return fmt.Sprintf("%s-mlserver", m.Name)
}

// Workflow Manager name (for config maps and deployment)
func (m *MiniMummi) WFManagerName() string {
	return fmt.Sprintf("%s-wfmanager", m.Name)
}

// Cluster Role and Role names
func (m *MiniMummi) ClusterRoleName() string {
	return fmt.Sprintf("%s-cluster-roles", m.Name)

}
func (m *MiniMummi) RoleName() string {
	return fmt.Sprintf("%s-roles", m.Name)
}

// RabbitName is used for the deployment and associated configmap
func (m *MiniMummi) RabbitName() string {
	return fmt.Sprintf("%s-rabbitmq", m.Name)
}

// SetRegistryDefaults ensure we have an image, port, name, etc.
func (m *MiniMummi) SetRegistryDefaults() {

	// If a custom registry is set, these variables are moot
	if !m.HasInClusterRegistry() {
		return
	}
	if m.Spec.Registry.Port == 0 {
		m.Spec.Registry.Port = defaultRegistryPort
	}
	fmt.Printf("🦛 MiniMummi.Spec.Registry %s\n", m.RegistryHost())
	if m.Spec.Registry.Name == "" {
		m.Spec.Registry.Name = defaultRegistryName
	}
	if m.Spec.Registry.Replicas == 0 {
		m.Spec.Registry.Replicas = 1
	}
}

// SetMLServerDefaults ensures we set defaults for the ML Server
// It seems to be a bug in kubebuilder they are not set
func (m *MiniMummi) SetMLServerDefaults() {

	// Validate MLServer Sampler
	if m.Spec.MLServer.Sampler.Interpolator == "" {
		m.Spec.MLServer.Sampler.Interpolator = defaultSamplerInterpolator
	}
	if m.Spec.MLServer.Validator.Complex == "" {
		m.Spec.MLServer.Validator.Complex = defaultValidatorComplex
	}
	if m.Spec.MLServer.Validator.Resources == "" {
		m.Spec.MLServer.Validator.Resources = defaultValidatorResources
	}
	if m.Spec.MLServer.Sampler.SubSampleFraction == "" {
		m.Spec.MLServer.Sampler.SubSampleFraction = defaultSamplerSubSampleFraction
	}
	if m.Spec.MLServer.Config.Logging.Level == 0 {
		m.Spec.MLServer.Config.Logging.Level = 2
	}
	if m.Spec.MLServer.Config.Logging.ToStdout == 0 {
		m.Spec.MLServer.Config.Logging.ToStdout = 1
	}
	if m.Spec.MLServer.Encoder.Path == "" {
		m.Spec.MLServer.Encoder.Path = fmt.Sprintf("%s/ml", m.Spec.Paths.MummiResources)
	}
	if m.Spec.MLServer.Encoder.Model == "" {
		m.Spec.MLServer.Encoder.Model = defaultEncoderModel
	}
	if m.Spec.MLServer.Encoder.Positions == "" {
		m.Spec.MLServer.Encoder.Positions = defaultEncoderPositions
	}
	if m.Spec.MLServer.Sampler.Feedback.Database == "" {
		m.Spec.MLServer.Sampler.Feedback.Database = defaultFeedbackDatabase
	}
	if m.Spec.MLServer.Sampler.Feedback.FrameDatabase == "" {
		m.Spec.MLServer.Sampler.Feedback.FrameDatabase = defaultFeedbackFrameDatabase
	}
	if m.Spec.MLServer.Sampler.FactorExtraStructures == 0 {
		m.Spec.MLServer.Sampler.FactorExtraStructures = defaultSamplerFactorExtraStructures
	}
	if m.Spec.MLServer.Sampler.Kneighbors == 0 {
		m.Spec.MLServer.Sampler.Kneighbors = defaultSamplerKneighbors
	}
	if m.Spec.MLServer.Sampler.LambdaUpperbound == 0 {
		m.Spec.MLServer.Sampler.LambdaUpperbound = defaultSamplerLambdaUpperBound
	}
	if m.Spec.MLServer.Sampler.NumberIterationsMax == 0 {
		m.Spec.MLServer.Sampler.NumberIterationsMax = defaultSamplerIterationsMax
	}
}

// SetMLServerDefaults ensures we set defaults for the ML Server
// It seems to be a bug in kubebuilder they are not set
func (m *MiniMummi) SetRabbitMQDefaults() {
	if m.Spec.RabbitMQ.Broker.Interface == "" {
		m.Spec.RabbitMQ.Broker.Interface = defaultBrokerInterface
	}
	if m.Spec.RabbitMQ.Broker.Queue == "" {
		m.Spec.RabbitMQ.Broker.Queue = defaultBrokerQueue
	}
}

// SetWFManagerDefaults sets defaults for the workflow manager
func (m *MiniMummi) SetWFManagerDefaults() {
	if m.Spec.WorkflowManager.IoType == "" {
		m.Spec.WorkflowManager.IoType = defaultSaveType
	}
	if m.Spec.WorkflowManager.FbType == "" {
		m.Spec.WorkflowManager.FbType = defaultSaveType
	}
	// Must be between 1 and 5
	if m.Spec.WorkflowManager.Logging.Level <= 0 {
		m.Spec.WorkflowManager.Logging.Level = 2
	}
}

// SetPathsDefaults sets the default paths
func (m *MiniMummi) SetPathsDefaults() {
	if m.Spec.Paths.Certs == "" {
		m.Spec.Paths.Certs = defaultPathCerts
	}
	if m.Spec.Paths.MummiResources == "" {
		m.Spec.Paths.MummiResources = defaultPathsResources
	}
	if m.Spec.Paths.MummiRoot == "" {
		m.Spec.Paths.MummiRoot = defaultPathsMummiRoot
	}

	fmt.Printf("🦛 MiniMummi.Spec.Paths.MummiRoot %s\n", m.Spec.Paths.MummiRoot)
	fmt.Printf("🦛 MiniMummi.Spec.Paths.MummiResources %s\n", m.Spec.Paths.MummiResources)
}

// Validate ensures we have data that is needed, and sets defaults if needed
func (m *MiniMummi) Validate() bool {
	fmt.Println()

	// Validate we've been provided containers (that are private)
	// These can eventually be replaced with defaults
	if m.Spec.MLServer.Image == "" {
		fmt.Println("👉 MiniMummi.Spec.MLServer.Image is not defined")
		return false
	}
	if m.Spec.RabbitMQ.Image == "" {
		fmt.Println("👉 MiniMummi.Spec.RabbitMQ.Image is not defined")
		return false
	}
	if m.Spec.WorkflowManager.Image == "" {
		fmt.Println("👉 MiniMummi.Spec.Workflow.Image is not defined")
		return false
	}
	if m.Spec.Registry.Image == "" {
		m.Spec.Registry.Image = "ghcr.io/oras-project/registry:latest"
	}

	// Registry, MLServer Defaults
	m.SetPathsDefaults()
	m.SetRegistryDefaults()
	m.SetMLServerDefaults()
	m.SetRabbitMQDefaults()

	// Validate MLServer Sampler
	interpolator := m.Spec.MLServer.Sampler.Interpolator
	if !slices.Contains(validSamplerInterpolator, interpolator) {
		fmt.Printf("👉 MiniMummi.Spec.MLServer.Sampler.Interpolator '%s' is invalid. Choices are '%s'\n", interpolator, validSamplerInterpolator)
		return false
	}

	// ioType and fbType validate to this set
	saveType := m.Spec.WorkflowManager.IoType
	if !slices.Contains(validSaveTypes, saveType) {
		fmt.Printf("👉 MiniMummi.Spec.WorkflowManager.IoType '%s' is invalid. Choices are '%s'\n", saveType, validSaveTypes)
		return false
	}
	saveType = m.Spec.WorkflowManager.FbType
	if !slices.Contains(validSaveTypes, saveType) {
		fmt.Printf("👉 MiniMummi.Spec.WorkflowManager.FbType '%s' is invalid. Choices are '%s'\n", saveType, validSaveTypes)
		return false
	}
	return true
}

// +kubebuilder:object:root=true

// MiniMummiList contains a list of MiniMummi
type MiniMummiList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []MiniMummi `json:"items"`
}

func init() {
	SchemeBuilder.Register(&MiniMummi{}, &MiniMummiList{})
}
