package desireclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/openshift-hyperfleet/hyperfleet-adapter/internal/manifest"
	"github.com/openshift-hyperfleet/hyperfleet-adapter/internal/transportclient"
	"github.com/openshift-hyperfleet/hyperfleet-adapter/pkg/desire"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Client implements transportclient.TransportClient using the desire-based delivery system.
// It takes plain Kubernetes manifests from the adapter framework, wraps them as desires,
// and writes them to the configured desire store. Status is read back from ReadDesire
// statuses populated by the applier agent on the target cluster.
type Client struct {
	specStore   desire.SpecStore
	statusStore desire.StatusStore
	logger      *slog.Logger
}

// NewClient creates a new desire transport client.
func NewClient(cfg Config, logger *slog.Logger) (*Client, error) {
	if cfg.SpecStore == nil {
		return nil, fmt.Errorf("spec store is required")
	}
	if cfg.StatusStore == nil {
		return nil, fmt.Errorf("status store is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{
		specStore:   cfg.SpecStore,
		statusStore: cfg.StatusStore,
		logger:      logger.With("transport", "desire"),
	}, nil
}

// ApplyResource takes a plain Kubernetes manifest (JSON bytes), wraps it as an
// ApplyDesire, and writes it to the spec store. Also creates a corresponding
// ReadDesire so the applier will mirror the live object's status back.
func (c *Client) ApplyResource(
	ctx context.Context,
	manifestBytes []byte,
	opts *transportclient.ApplyOptions,
	target transportclient.TransportContext,
) (*transportclient.ApplyResult, error) {
	tc, err := c.extractContext(target)
	if err != nil {
		return nil, err
	}

	// Parse the manifest to extract identity
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(manifestBytes); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	targetItem := targetItemFromUnstructured(obj)
	desireID := desire.DesireID{
		Partition: tc.Partition,
		Name:      desireNameFromTargetItem(targetItem),
	}

	c.logger.Info("applying desire",
		"partition", tc.Partition,
		"desire", desireID.Name,
		"gvk", obj.GroupVersionKind().String(),
		"resource", obj.GetName(),
	)

	// Try to get existing desire to determine create vs update
	existing, err := c.specStore.GetApplyDesire(ctx, desireID)
	if err != nil && !errors.Is(err, desire.ErrNotFound) {
		return nil, fmt.Errorf("get existing apply desire: %w", err)
	}

	applyDesire := &desire.ApplyDesire{
		ID: desireID,
		Spec: desire.ApplyDesireSpec{
			KubeContent: manifestBytes,
			TargetItem:  targetItem,
		},
	}

	var op manifest.Operation
	if existing == nil {
		// Create new desire
		if err := c.specStore.CreateApplyDesire(ctx, applyDesire); err != nil {
			return nil, fmt.Errorf("create apply desire: %w", err)
		}
		op = manifest.OperationCreate
	} else {
		// Update existing desire spec
		if err := c.specStore.UpdateApplyDesireSpec(ctx, desireID, applyDesire.Spec, existing.Version); err != nil {
			return nil, fmt.Errorf("update apply desire spec: %w", err)
		}
		op = manifest.OperationUpdate
	}

	// Ensure a ReadDesire exists for this resource so status flows back
	if err := c.ensureReadDesire(ctx, desireID, targetItem); err != nil {
		c.logger.Warn("failed to ensure read desire", "error", err, "desire", desireID.Name)
		// Non-fatal: the apply will still work, status mirroring just won't happen
	}

	return &transportclient.ApplyResult{
		Operation: op,
		Reason:    fmt.Sprintf("desire %s written to store", op),
	}, nil
}

// GetResource reads the corresponding ReadDesire's status.kubeContent from the status
// store and returns the mirrored live object. Returns not-found if the ReadDesire
// doesn't exist or has no kubeContent yet (agent hasn't synced).
func (c *Client) GetResource(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	namespace, name string,
	target transportclient.TransportContext,
) (*unstructured.Unstructured, error) {
	tc, err := c.extractContext(target)
	if err != nil {
		return nil, err
	}

	targetItem := desire.TargetItem{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Resource:  strings.ToLower(gvk.Kind) + "s", // rough pluralization for POC
		Namespace: namespace,
		Name:      name,
	}
	desireID := desire.DesireID{
		Partition: tc.Partition,
		Name:      desireNameFromTargetItem(targetItem),
	}

	rd, err := c.statusStore.GetReadDesire(ctx, desireID)
	if err != nil {
		if errors.Is(err, desire.ErrNotFound) {
			// Return a K8s-compatible NotFound error so callers (e.g., preDiscoverAll)
			// treat this as a non-fatal "resource doesn't exist yet" signal.
			return nil, apierrors.NewNotFound(
				schema.GroupResource{Group: gvk.Group, Resource: strings.ToLower(gvk.Kind) + "s"},
				name,
			)
		}
		return nil, fmt.Errorf("get read desire: %w", err)
	}

	if rd.Status.KubeContent == nil {
		// No content yet means the agent hasn't synced. Treat as not found.
		return nil, apierrors.NewNotFound(
			schema.GroupResource{Group: gvk.Group, Resource: strings.ToLower(gvk.Kind) + "s"},
			name,
		)
	}

	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(rd.Status.KubeContent); err != nil {
		return nil, fmt.Errorf("unmarshal read desire kube content: %w", err)
	}
	return obj, nil
}

// DiscoverResources lists desires by partition from the status store, filters by GVK,
// and returns matched resources from ReadDesire statuses.
func (c *Client) DiscoverResources(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	discovery manifest.Discovery,
	target transportclient.TransportContext,
) (*unstructured.UnstructuredList, error) {
	tc, err := c.extractContext(target)
	if err != nil {
		return nil, err
	}

	readDesires, err := c.statusStore.ListReadDesires(ctx, tc.Partition)
	if err != nil {
		return nil, fmt.Errorf("list read desires: %w", err)
	}

	result := &unstructured.UnstructuredList{}
	for _, rd := range readDesires {
		if rd.Status.KubeContent == nil {
			continue
		}

		obj := &unstructured.Unstructured{}
		if err := obj.UnmarshalJSON(rd.Status.KubeContent); err != nil {
			continue
		}

		// Check GVK matches
		if obj.GroupVersionKind() != gvk {
			continue
		}

		// Check discovery criteria
		if manifest.MatchesDiscoveryCriteria(obj, discovery) {
			result.Items = append(result.Items, *obj)
		}
	}

	return result, nil
}

// DeleteResource creates a DeleteDesire in the spec store and removes the
// associated ApplyDesire so the applier stops re-applying the resource. The
// ReadDesire is intentionally retained: it lets the applier continue mirroring
// the resource's state (and absence) while deletion is in flight. The
// DeleteDesire and ReadDesire are cleaned up by the adapter (via
// CleanupDeleteDesire / CleanupReadDesire) only after the applier reports
// Successful=True on the DeleteDesire.
func (c *Client) DeleteResource(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	namespace, name string,
	opts *transportclient.DeleteOptions,
	target transportclient.TransportContext,
) error {
	tc, err := c.extractContext(target)
	if err != nil {
		return err
	}

	targetItem := desire.TargetItem{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Resource:  strings.ToLower(gvk.Kind) + "s",
		Namespace: namespace,
		Name:      name,
	}
	desireID := desire.DesireID{
		Partition: tc.Partition,
		Name:      desireNameFromTargetItem(targetItem),
	}

	c.logger.Info("deleting via desire",
		"partition", tc.Partition,
		"desire", desireID.Name,
		"resource", name,
	)

	// Create a DeleteDesire — the applier will pick this up, delete the K8s resource,
	// and write Successful=True.
	dd := &desire.DeleteDesire{
		ID: desireID,
		Spec: desire.DeleteDesireSpec{
			TargetItem: targetItem,
		},
	}
	if err := c.specStore.CreateDeleteDesire(ctx, dd); err != nil {
		if !errors.Is(err, desire.ErrVersionConflict) {
			return fmt.Errorf("create delete desire: %w", err)
		}
		// Already exists, that's fine
	}

	// Stop the applier from re-applying. ReadDesire stays so the applier can
	// keep mirroring (and ultimately report absence). Final cleanup of the
	// DeleteDesire and ReadDesire happens after the applier confirms via
	// Successful=True on the DeleteDesire.
	_ = c.specStore.DeleteApplyDesire(ctx, desireID)

	return nil
}

// ensureReadDesire creates a ReadDesire if one doesn't already exist.
func (c *Client) ensureReadDesire(ctx context.Context, id desire.DesireID, target desire.TargetItem) error {
	rd := &desire.ReadDesire{
		ID: id,
		Spec: desire.ReadDesireSpec{
			TargetItem: target,
		},
	}
	err := c.specStore.CreateReadDesire(ctx, rd)
	if err != nil && !errors.Is(err, desire.ErrVersionConflict) {
		return err
	}
	return nil
}

// extractContext type-asserts the TransportContext to a DesireTransportContext.
func (c *Client) extractContext(target transportclient.TransportContext) (*TransportContext, error) {
	if target == nil {
		return nil, fmt.Errorf("desire transport requires a TransportContext with Partition")
	}
	tc, ok := target.(*TransportContext)
	if !ok {
		return nil, fmt.Errorf("expected *desireclient.TransportContext, got %T", target)
	}
	if tc.Partition == "" {
		return nil, fmt.Errorf("desire transport context partition cannot be empty")
	}
	return tc, nil
}

// targetItemFromUnstructured extracts a TargetItem from an unstructured K8s object.
func targetItemFromUnstructured(obj *unstructured.Unstructured) desire.TargetItem {
	gvk := obj.GroupVersionKind()
	return desire.TargetItem{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Resource:  strings.ToLower(gvk.Kind) + "s", // rough pluralization for POC
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

// desireNameFromTargetItem builds a desire name from a TargetItem.
// Format: {resource}.{namespace}.{name} or {resource}.{name} for cluster-scoped.
func desireNameFromTargetItem(item desire.TargetItem) string {
	if item.Namespace != "" {
		return fmt.Sprintf("%s.%s.%s", item.Resource, item.Namespace, item.Name)
	}
	return fmt.Sprintf("%s.%s", item.Resource, item.Name)
}

// IsDeleteConfirmed reads the DeleteDesire status from the StatusStore and
// returns whether the applier has confirmed the K8s resource is gone.
//
//   - confirmed=true when the DeleteDesire's Successful condition is True.
//   - exists=false when the DeleteDesire has already been removed from the store
//     (a prior event cleaned it up, or it was never created). Callers should
//     treat this the same as confirmed=true.
//   - err is returned only for transient store errors; ErrNotFound is mapped to
//     exists=false with a nil error.
func (c *Client) IsDeleteConfirmed(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	namespace, name string,
	target transportclient.TransportContext,
) (confirmed bool, exists bool, err error) {
	tc, err := c.extractContext(target)
	if err != nil {
		return false, false, err
	}

	id := desireIDFor(tc.Partition, gvk, namespace, name)

	dd, err := c.statusStore.GetDeleteDesire(ctx, id)
	if err != nil {
		if errors.Is(err, desire.ErrNotFound) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("get delete desire: %w", err)
	}

	for _, cond := range dd.Status.Conditions {
		if cond.Type == desire.ConditionTypeSuccessful {
			return cond.Status == metav1.ConditionTrue, true, nil
		}
	}
	return false, true, nil
}

// desireIDFor builds the DesireID for a target K8s resource on a partition.
// Mirrors the id shape used by ApplyResource/DeleteResource so reads and writes
// match by key.
func desireIDFor(partition string, gvk schema.GroupVersionKind, namespace, name string) desire.DesireID {
	item := desire.TargetItem{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Resource:  strings.ToLower(gvk.Kind) + "s",
		Namespace: namespace,
		Name:      name,
	}
	return desire.DesireID{
		Partition: partition,
		Name:      desireNameFromTargetItem(item),
	}
}

// CleanupReadDesire removes a ReadDesire from the store after confirming
// the resource has been deleted. Called alongside CleanupDeleteDesire.
func (c *Client) CleanupReadDesire(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	namespace, name string,
	target transportclient.TransportContext,
) error {
	tc, err := c.extractContext(target)
	if err != nil {
		return err
	}

	targetItem := desire.TargetItem{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Resource:  strings.ToLower(gvk.Kind) + "s",
		Namespace: namespace,
		Name:      name,
	}
	desireID := desire.DesireID{
		Partition: tc.Partition,
		Name:      desireNameFromTargetItem(targetItem),
	}

	return c.specStore.DeleteReadDesire(ctx, desireID)
}

// CleanupDeleteDesire removes a DeleteDesire from the store after confirming
// the resource has been deleted. Called by the executor after post-delete
// discovery confirms the resource is gone.
func (c *Client) CleanupDeleteDesire(
	ctx context.Context,
	gvk schema.GroupVersionKind,
	namespace, name string,
	target transportclient.TransportContext,
) error {
	tc, err := c.extractContext(target)
	if err != nil {
		return err
	}

	targetItem := desire.TargetItem{
		Group:     gvk.Group,
		Version:   gvk.Version,
		Resource:  strings.ToLower(gvk.Kind) + "s",
		Namespace: namespace,
		Name:      name,
	}
	desireID := desire.DesireID{
		Partition: tc.Partition,
		Name:      desireNameFromTargetItem(targetItem),
	}

	c.logger.Info("cleaning up delete desire after confirmed deletion",
		"partition", tc.Partition,
		"desire", desireID.Name,
	)

	return c.specStore.DeleteDeleteDesire(ctx, desireID)
}

// Compile-time interface check.
var _ transportclient.TransportClient = (*Client)(nil)

// marshalUnstructured is a helper for JSON serialization of unstructured objects.
func marshalUnstructured(obj *unstructured.Unstructured) ([]byte, error) {
	return json.Marshal(obj.Object)
}
