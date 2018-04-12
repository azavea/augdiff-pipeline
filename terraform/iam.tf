#
# Spot Fleet IAM resources
#
data "aws_iam_policy_document" "container_instance_spot_fleet_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["spotfleet.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "container_instance_spot_fleet" {
  name               = "fleet${var.environment}ServiceRole"
  assume_role_policy = "${data.aws_iam_policy_document.container_instance_spot_fleet_assume_role.json}"
}

resource "aws_iam_role_policy_attachment" "spot_fleet_policy" {
  role       = "${aws_iam_role.container_instance_spot_fleet.name}"
  policy_arn = "${var.aws_spot_fleet_service_role_policy_arn}"
}


#
# Batch IAM resources
#
data "aws_iam_policy_document" "container_instance_batch_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["batch.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "container_instance_batch" {
  name               = "batch${var.environment}ServiceRole"
  assume_role_policy = "${data.aws_iam_policy_document.container_instance_batch_assume_role.json}"
}

resource "aws_iam_role_policy_attachment" "batch_policy" {
  role       = "${aws_iam_role.container_instance_batch.name}"
  policy_arn = "${var.aws_batch_service_role_policy_arn}"
}

data "aws_iam_policy_document" "ecs_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["ecs.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ecs_service_role" {
  name               = "ecs${var.environment}ServiceRole"
  assume_role_policy = "${data.aws_iam_policy_document.ecs_assume_role.json}"
}

resource "aws_iam_role_policy_attachment" "ecs_service_role" {
  role       = "${aws_iam_role.ecs_service_role.name}"
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceRole"
}

