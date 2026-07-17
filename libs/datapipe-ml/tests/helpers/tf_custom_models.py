from __future__ import annotations

from typing import List, Tuple


def build_tiny_custom_classifier(
    image_size: Tuple[int, int],
    class_names: List[str],
    filters: int = 4,
    activation: str = "relu",
):
    import tensorflow as tf

    return tf.keras.Sequential(
        [
            tf.keras.layers.Input(shape=(image_size[1], image_size[0], 3)),
            tf.keras.layers.Conv2D(filters, 3, activation=activation),
            tf.keras.layers.GlobalAveragePooling2D(),
            tf.keras.layers.Dense(len(class_names), activation="softmax"),
        ]
    )
