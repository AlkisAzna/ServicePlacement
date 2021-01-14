# ServicesPlacement
A Service Placement algorithm to produce a better Clustering of Pods inside Nodes (VMs) according to Node Affinities of an App called iXen running on Kubernetes Engine on GCP
INFO: The Kubernetes Cluster can run and be processed from any engine host including local process like minikube.

## Setting up Kubernetes Cluster
1) Create a Kubernetes Cluster 
2) Deploy the App

## Get Cluster Information
Run **kubectl get pods -o wide > <file_name>.<file_format>** to get the configuration of Cluster Pods and the Nodes they are running in.

## Current Repository
Run **kubectl get pods -o wide > pods_configuration.txt** and then run python main_system.py.
INFO: At the time we support only txt formats
