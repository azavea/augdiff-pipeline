
resource "aws_batch_job_queue" "default" {
  name                 = "queue${var.environment}Default"
  priority             = 1
  state                = "ENABLED"
  compute_environments = ["${aws_batch_compute_environment.default.arn}"]
}

