
resource "aws_db_instance" "metastore" {
  allocated_storage    = 10
  storage_type         = "gp2"
  engine               = "postgres"
  engine_version       = "9.6"
  instance_class       = "db.t2.micro"
  name                 = "metastore"
  username             = "hive"
  password             = "hivehive"
}
