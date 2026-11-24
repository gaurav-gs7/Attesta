-- The API no longer records rollouts or deployments. Those rows described
-- requests made to the API, not anything that happened in a cluster: a
-- "rollback" here never changed a Deployment. What the controller actually
-- did is in the cluster, as hash-chained RolloutDecision objects, which the
-- API reads. rollouts references deployments, so it goes first.
DROP TABLE IF EXISTS rollouts;
DROP TABLE IF EXISTS deployments;
