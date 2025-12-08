MapReduce Worker EC2 Setup Guide

This document is a checklist for bringing up a new worker EC2 instance that can talk to the existing coordinator and S3-backed storage.

It assumes:
	•	Coordinator is already running on EC2 in us-east-1
	•	Coordinator + S3 are working
	•	Repo: map_reduce
	•	Bucket: rc-mapreduce-bucket (swap if you change)

⸻

1. Launch a Worker EC2 Instance

In the AWS Console:
	1.	Go to EC2 → Instances → Launch instance
	2.	Name: mr-worker-<n> (e.g. mr-worker-1)
	3.	AMI: Ubuntu 22.04 LTS
	4.	Instance type: t3.small (what you used)
	5.	Key pair: same key pair as the coordinator (e.g. mr-single.pem)
	6.	Network:
	•	Same VPC and subnet as the coordinator
	7.	Security Group:
	•	Reuse the same security group as the coordinator
(so workers + coordinator can talk on port 8123)
	8.	IAM Role:
	•	Use the same IAM role as the coordinator (e.g. mr-s3-role with S3 access)
	9.	Launch the instance.

⸻

2. SSH Into the Worker

On your laptop, make sure the key is in ~/.ssh with correct perms:

mv /path/to/mr-single.pem ~/.ssh/  # only if it's not there already
chmod 600 ~/.ssh/mr-single.pem

Then SSH (Ubuntu instance):

ssh -i ~/.ssh/mr-single.pem ubuntu@<WORKER_PUBLIC_IP>


⸻

3. Install Basic Tools

On the worker:

sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl wget unzip build-essential


⸻

4. Install Go (same version as local)

cd ~
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz

# Add Go to PATH
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc

go version   # should show 1.24.x


⸻

5. Install AWS CLI v2

cd ~
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version

No aws configure needed because the instance has an IAM role.

Check that the role is visible:

aws sts get-caller-identity

You should see an ARN with your mr-s3-role.

⸻

6. Set Region Environment Variables

You’re using us-east-1, so make it permanent:

echo 'export AWS_REGION=us-east-1' >> ~/.bashrc
echo 'export AWS_DEFAULT_REGION=us-east-1' >> ~/.bashrc
source ~/.bashrc

echo $AWS_REGION      # should print us-east-1
echo $AWS_DEFAULT_REGION


⸻

7. Enable CGO (required for Go plugins)

Plugins (.so files) need CGO and a C toolchain (you installed build-essential already).

Add to ~/.bashrc:

echo 'export CGO_ENABLED=1' >> ~/.bashrc
source ~/.bashrc

go env CGO_ENABLED    # should be 1


⸻

8. Clone the Repo and Make Scripts Executable

cd ~
git clone https://github.com/LakshyaMittal3301/mapreduce.git
cd mapreduce

Make scripts executable (coordinator + worker runners, single-run, etc.):

chmod +x scripts/*.sh

(If you add more scripts later, you can re-run that.)

⸻

9. Sanity Check: Can Worker Reach Coordinator?

Get the private IPv4 of the coordinator from the EC2 console (e.g. 172.31.22.140).

From the worker:

nc -vz 172.31.22.140 8123

You want:

Connection to 172.31.22.140 8123 port [tcp/*] succeeded!

If it fails:
	•	Check the coordinator security group has inbound rule:
	•	Type: Custom TCP
	•	Port: 8123
	•	Source: your worker’s security group (or same SG)

⸻

10. Start a Worker (example)

From repo root on the worker:

First build the plugin and worker binary (if your scripts don’t do it for you):

cd ~/mapreduce

# plugin
cd apps
go build -buildmode=plugin -o ../bin/plugins/wc.so wc.go
cd ..

# worker binary
cd cmd
go build -o ../bin/mrworker mrworker.go
cd ..

Then run a worker pointing at the coordinator’s private IP:

./bin/mrworker \
  -coord-addr="172.31.22.140:8123" \
  -storage="s3" \
  -s3-bucket="rc-mapreduce-bucket" \
  -s3-input-prefix="inputs/pg" \
  -s3-concurrency=16 \
  -idle-wait=1s \
  -log-level=info \
  -app="./bin/plugins/wc.so"

Or, if you’re using your helper script (e.g. run-workers-ec2.sh):

./scripts/run-workers-ec2.sh \
  wc \
  rc-mapreduce-bucket \
  172.31.22.140:8123 \
  3


⸻

11. Quick Checklist for a New Worker

When spinning up a new worker instance, you should be able to just:
	1.	Launch EC2 (Ubuntu, t3.small, same SG + IAM + key)
	2.	SSH in
	3.	Run:

sudo apt update && sudo apt install -y git curl wget unzip build-essential
# install Go
wget https://go.dev/dl/go1.24.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.24.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
echo 'export AWS_REGION=us-east-1' >> ~/.bashrc
echo 'export AWS_DEFAULT_REGION=us-east-1' >> ~/.bashrc
echo 'export CGO_ENABLED=1' >> ~/.bashrc
source ~/.bashrc

# clone repo
git clone https://github.com/LakshyaMittal3301/mapreduce.git
cd mapreduce
chmod +x scripts/*.sh


	4.	nc -vz <coord-private-ip> 8123 to confirm connectivity
	5.	Start workers with your preferred command/script
