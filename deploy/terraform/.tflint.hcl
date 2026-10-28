# tflint config for both Terraform stacks:
#   tflint --chdir=deploy/terraform/bootstrap --config=$PWD/deploy/terraform/.tflint.hcl
plugin "terraform" {
  enabled = true
  preset  = "recommended"
}

plugin "aws" {
  enabled = true
  version = "0.50.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
