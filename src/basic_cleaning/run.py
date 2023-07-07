#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):


    logger.info("Initializing new run")
    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)


    artifact_local_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_local_path)
    price_filter = df["price"].between(args.min_price,args.max_price)
    min_nights_filter = df["minimum_nights"].between(1,14)

    new_df = df[price_filter & min_nights_filter].copy()
    new_df["last_review"] = pd.to_datetime(new_df["last_review"])
    idx = new_df['longitude'].between(-74.25, -73.50) & new_df['latitude'].between(40.5, 41.2)
    new_df = new_df[idx].copy()
    new_df.to_csv("clean_sample.csv", index = False)

    art = wandb.Artifact(
        args.output_artifact,
        args.output_type,
        args.output_description
    )

    art.add_file("clean_sample.csv")

    run.log_artifact(art)

    logger.info(f"Step completed successfully. Added new artifact {args.output_artifact}.")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic cleaning step")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="The input  artifact to use",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="The name of the output artifact created",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="The type of artifact that will be output",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="The description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="The minimum price accepted as a valid data point",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="the maximum price accepted as a valid data point",
        required=True
    )


    args = parser.parse_args()

    go(args)
