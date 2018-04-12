resource "aws_batch_compute_environment" "default" {
  compute_environment_name = "batch${var.environment}DefaultComputeEnvironment"
  type                     = "MANAGED"
  state                    = "ENABLED"
  service_role             = "${aws_iam_role.container_instance_batch.arn}"

  compute_resources {
    type           = "SPOT"
    bid_percentage = "${var.spot_fleet_bid_percentage}"
    ec2_key_pair   = "${var.aws_key_name}"
    # TODO look into ephemeral drives/defining custom AMI (block device mapping)
    # image_id       = "${var.batch_ami_id}"

    min_vcpus     = "0"
    max_vcpus     = "16"

    spot_iam_fleet_role = "${aws_iam_role.container_instance_spot_fleet.arn}"
    instance_role       = "${aws_iam_role.ecs_service_role.arn}"

    instance_type = [ "c3" ]

    security_group_ids = [ "${aws_security_group.container_instance.id}" ]

    subnets = [ "${var.public_subnets}", ]

    tags {
      Name               = "BatchWorker"
      ComputeEnvironment = "Default"
      Project            = "${var.project}"
      Environment        = "${var.environment}"
    }
  }
}

data "template_file" "adi_update" {
  template = "${file("job-definitions/batch-adi-update.json")}"

  vars {
    #batch_image_url               = "896538046175.dkr.ecr.us-east-1.amazonaws.com/adi-update:latest"
    batch_image_url               = "${aws_ecr_repository.adi_update.repository_url}:latest"
    #batch_image_url               = "busybox:latest"
    aws_region                    = "${var.aws_region}"
    environment                   = "${var.environment}"
  }
}

resource "aws_batch_job_definition" "adi_update" {
  name = "job${var.environment}AdiUpdate"
  type = "container"

  container_properties = "${data.template_file.adi_update.rendered}"

  parameters {
  }

  retry_strategy {
    attempts = 3
  }
}

resource "aws_ecr_repository" "adi_update" {
  name = "adi-update"
}

data "template_file" "augdiff_gen" {
  template = "${file("job-definitions/batch-adi-update.json")}"

  vars {
    #batch_image_url               = "896538046175.dkr.ecr.us-east-1.amazonaws.com/augdiff-gen:latest"
    batch_image_url               = "${aws_ecr_repository.adi_update.repository_url}:latest"
    #batch_image_url               = "busybox:latest"
    aws_region                    = "${var.aws_region}"
    environment                   = "${var.environment}"
  }
}

resource "aws_batch_job_definition" "augdiff_gen" {
  name = "job${var.environment}AugDiffGen"
  type = "container"

  container_properties = "${data.template_file.augdiff_gen.rendered}"

  parameters {
  }

  retry_strategy {
    attempts = 3
  }
}

resource "aws_ecr_repository" "augdiff_gen" {
  name = "augdiff-gen"
}
