package workflows

import "context"

type actorKey struct{}

// WithActor records who is starting runs with ctx. Runs started with it
// carry the actor, so every run names its caller.
func WithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

// ActorFrom returns the actor in ctx, or "system".
func ActorFrom(ctx context.Context) string {
	if a, ok := ctx.Value(actorKey{}).(string); ok && a != "" {
		return a
	}
	return "system"
}
