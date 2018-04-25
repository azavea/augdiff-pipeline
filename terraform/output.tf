

output "adi_container_address" {
  value = "${aws_ecr_repository.adi_update.repository_url}"
}

output "augdiff_container_address" {
  value = "${aws_ecr_repository.augdiff_gen.repository_url}"
}
