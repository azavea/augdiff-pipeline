provider "aws" {
  region     = "${var.region}"
}

resource "aws_db_instance" "postgresql" {
  allocated_storage          = "${var.allocated_storage}"
  engine                     = "postgres"
  engine_version             = "${var.engine_version}"
  instance_class             = "${var.instance_class}"
  storage_type               = "${var.storage_type}"
  name                       = "${var.database_name}"
  username                   = "${var.database_username}"
  password                   = "${var.database_password}"
  port                       = "${var.port}"
  vpc_security_group_ids     = ["${aws_security_group.security-group.id}"]
  skip_final_snapshot        = true
  publicly_accessible        = true # XXX
}
