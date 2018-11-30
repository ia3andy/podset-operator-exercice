package podset

import (
	"context"
	andyv1alpha1 "ia3andy/podset-operator/pkg/apis/andy/v1alpha1"
	"log"
	"reflect"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new PodSet Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcilePodSet{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("podset-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource PodSet
	err = c.Watch(&source.Kind{Type: &andyv1alpha1.PodSet{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner PodSet
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &andyv1alpha1.PodSet{},
	})
	if err != nil {
		return err
	}

	return nil
}

var _ reconcile.Reconciler = &ReconcilePodSet{}

// ReconcilePodSet reconciles a PodSet object
type ReconcilePodSet struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a PodSet object and makes changes based on the state read
// and what is in the PodSet.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcilePodSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	log.Printf("Reconciling PodSet %s/%s\n", request.Namespace, request.Name)

	// Fetch the PodSet instance
	instance := &andyv1alpha1.PodSet{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	replicas := int(instance.Spec.Replicas)
	log.Printf("PodSet Spec.Replicas is %d", replicas)

	// Define a new Pod object
	pod := newPodForCR(instance)

	// Set PodSet instance as the owner and controller
	if err := controllerutil.SetControllerReference(instance, pod, r.scheme); err != nil {
		return reconcile.Result{}, err
	}

	pods, err := listPods(instance, r)
	if err != nil {
		return reconcile.Result{}, err
	}
	countPods := len(pods.Items)

	log.Printf("List with size: %d", countPods)

	if countPods < replicas {
		log.Printf("Creating a new Pod %s/%s\n", pod.Namespace, pod.GenerateName)

		err = r.client.Create(context.TODO(), pod)
		if err != nil {
			return reconcile.Result{}, err
		}
	} else if countPods > replicas {
		//TODO delete a replica that is not already terminating
		podToDelete := &pods.Items[countPods-1]
		log.Printf("Too many pods deleting the last one %s/%s\n", podToDelete.Namespace, podToDelete.Name)
		err = r.client.Delete(context.TODO(), podToDelete)
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	podsAfterProcess, err := listPods(instance, r)

	if err != nil {
		return reconcile.Result{}, err
	}

	podNames := getPodNames(podsAfterProcess.Items)

	if !reflect.DeepEqual(podNames, instance.Status.PodNames) {
		instance.Status.PodNames = podNames
		err = r.client.Update(context.TODO(), instance)
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	// Pod process successfully terminated - don't requeue
	return reconcile.Result{}, nil
}

func getPodNames(pods []corev1.Pod) []string {
	var podNames []string
	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}
	return podNames
}

func listPods(cr *andyv1alpha1.PodSet, r *ReconcilePodSet) (*corev1.PodList, error) {
	pods := &corev1.PodList{}

	labelSelector := labels.SelectorFromSet(labelsForCR(cr))
	listOps := &client.ListOptions{
		Namespace:     cr.Namespace,
		LabelSelector: labelSelector,
	}

	/** another approach:
		listOps := &client.ListOptions{
			Namespace:     cr.Namespace,
			LabelSelector: labelSelector,
		}
	    listOps.SetLabelSelector(fmt.Sprintf("%s=%s", POD_LABEL_NAME, cr.Name))
	    err := r.client.List(context.TODO(), opts, podList)
	**/

	err := r.client.List(context.TODO(), listOps, pods)

	return pods, err
}

func labelsForCR(cr *andyv1alpha1.PodSet) map[string]string {
	return map[string]string{
		"app": cr.Name,
	}
}

// newPodForCR returns a busybox pod with the same name/namespace as the cr
func newPodForCR(cr *andyv1alpha1.PodSet) *corev1.Pod {
	labels := labelsForCR(cr)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cr.Name + "-pod",
			Namespace:    cr.Namespace,
			Labels:       labels,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:    "busybox",
					Image:   "busybox",
					Command: []string{"sleep", "3600"},
				},
			},
		},
	}
}
