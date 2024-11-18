# Databricks notebook source
import mlflow
mlflow.autolog(disable=True)
mlflow.set_registry_uri("databricks-uc")
