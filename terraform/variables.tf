variable "allocated_storage" {
  type        = "string"
  description = "The database size in gigabytes"
  default     = "8"
}

variable "engine_version" {
  type        = "string"
  description = "The database engine version"
  default     = "10.3"
}

variable "instance_class" {
  type        = "string"
  description = "The type of instance on which the database runs"
  default     = "db.t2.micro"
}

variable "storage_type" {
  type        = "string"
  description = "The type of storage backing the database"
  default     = "standard"
}

variable "port" {
  type        = "string"
  description = "The port number on-which the datbase can be connected to"
  default     = "5432"
}

variable "database_name" {
  type        = "string"
  description = "The name of the automatically-created database"
  default     = "metastore"
}

variable "database_username" {
  type        = "string"
  description = "The database login username"
}

variable "database_password" {
  type        = "string"
  description = "The database login password"
}

variable "region" {
  type        = "string"
  description = "AWS Region"
  default     = "us-east-1"
}
