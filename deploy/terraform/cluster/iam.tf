# The nodes' instance role: SSM agent only (AmazonSSMManagedInstanceCore).
# The bootstrap deploy role may create roles named <project>-node* and
# attach only that policy.
resource "aws_iam_role" "node" {
  name        = "${var.project}-node"
  description = "paxosd nodes: SSM agent only"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "node_ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "node" {
  name = "${var.project}-node"
  role = aws_iam_role.node.name
}
