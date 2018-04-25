
resource "aws_security_group" "container_instance" {
  vpc_id = "${var.vpc_id}"

  tags {
    Name        = "sgContainerInstance"
    Project     = "${var.project}"
    Environment = "${var.environment}"
  }
}

resource "aws_security_group_rule" "ssh_ingress" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["${var.external_access_cidr_block}"]
  security_group_id = "${aws_security_group.container_instance.id}"
}

resource "aws_security_group_rule" "http_egress" {
  type              = "egress"
  from_port         = 80
  to_port           = 80
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.container_instance.id}"
}

resource "aws_security_group_rule" "https_egress" {
  type              = "egress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = "${aws_security_group.container_instance.id}"
}
