module "s3" {
  source = "./modules/s3"

  datalake_bucket_name = "stedi-datalake-terraform-kr"
}

module "iam" {
  source = "./modules/iam"

  role_name            = "glue-stedi-role"
  datalake_bucket_name = module.s3.datalake_bucket_name
}

module "glue" {
  source               = "./modules/glue"
  database_name        = "stedi_lake"
  datalake_bucket_name = module.s3.datalake_bucket_name
  glue_role_arn        = module.iam.glue_role_arn
}

resource "null_resource" "trigger_all_jobs" {
  provisioner "local-exec" {
    command = "cmd /C run_glue_jobs.bat"
  }

  depends_on = [
    module.glue
  ]
}








# resource "null_resource" "trigger_all_jobs" {
#   provisioner "local-exec" {
#     command = "bash ./run_glue_jobs.sh"
#   }

#   depends_on = [
#     module.glue
#   ]
# }
