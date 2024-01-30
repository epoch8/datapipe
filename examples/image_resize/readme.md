# Image Resizing with Datapipe

## Overview

This repository presents a practical example demonstrating the use of Datapipe for image resizing. The example showcases how images placed in an "input" folder are resized and managed by Datapipe, making it ideal for dynamic content management scenarios such as social media platforms.

The primary objective is to ensure efficient handling of image data. The system is designed to resize images added to the input folder and to handle deletions effectively - if an image is removed (e.g. due to content moderation), its resized versions are also automatically deleted.

## Description

This example is a real-world application of Datapipe in a content moderation setting. The focus is on:

- **Automatic Content Tracking:** Datapipe tracks changes in the folder, resizing new or modified images and deleting resizes of removed images.
- **Efficient Data Management:** The example demonstrates how Datapipe handles the addition, modification, and deletion of images, ensuring efficient data management in a content moderation workflow.

## Challenges Addressed

- **Content Moderation:** Automatically delete resized images when the original content is flagged and removed.
- **Minimalistic Function Design:** The resizing function is designed to be simple, focusing on the core task without managing processing statuses.
- The example can be adapted to run in real-time.

## Solution with Datapipe

Using the Datapipe library, this example demonstrates:

- **Dependency Tracking:** Automatically tracks and manages changes in the content, including additions and deletions.

## Prerequisites

- Python 3.8+
- Sqlalchemy 1.4 or 2.0
- Poetry for dependency management

## Setup

1. Clone this repository.
2. Navigate to this directory.
3. Install dependencies using Poetry (`poetry install`).

## Usage

To run the example:

1. Create the necessary SQLite databases by running `datapipe db create-all`.
2. Execute the data processing with `datapipe run`.

After each run, Datapipe will check the 'input' folder for new images, resize them, and manage the resized images in accordance with the changes in the 'input' folder, ensuring efficient content management.

## Conclusion

This example illustrates how Datapipe can be effectively utilized in real-time image processing and content moderation scenarios. Its ability to handle dynamic data changes with minimal manual intervention makes it a valuable tool for developers and content managers alike.
