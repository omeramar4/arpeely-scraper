import asyncio
import logging
import threading
from typing import List

from transformers import pipeline


class TopicClassifier:
    """
    Topic classifier using Hugging Face models for zero-shot classification.
    Loads the model once at initialization and supports concurrent classification.
    """

    TOPICS = [
        "weather", "news", "technology", "sports",
        "entertainment", "travel", "cooking", "politics",
        "shopping", "productivity", "coding", "other"
    ]

    # Recommended model for torch 2.2.2 compatibility (uses safetensors)
    RECOMMENDED_MODEL = "typeform/distilbert-base-uncased-mnli"

    def __init__(self, model_name: str):
        """
        Initialize the topic classifier with a specified Hugging Face model.

        Args:
            model_name: Name of the Hugging Face model to use for classification
        """
        self.logger = logging.getLogger(__name__)
        self.model_name = model_name
        self.classifier = None
        self._lock = threading.Lock()
        self._initialize_model()

    @classmethod
    def create_default(cls) -> 'TopicClassifier':
        """
        Create a TopicClassifier with the recommended model for torch 2.2.2.

        Returns:
            TopicClassifier instance with facebook/bart-large-mnli model
        """
        return cls(cls.RECOMMENDED_MODEL)

    def _initialize_model(self):
        """Initialize the classification model."""
        try:
            self.logger.info(f"Loading topic classification model: {self.model_name}...")
            self.classifier = pipeline(
                "zero-shot-classification",
                model=self.model_name,
                device=-1  # Use CPU to avoid GPU memory issues
            )
            self.logger.info(f"Topic classification model {self.model_name} loaded successfully")
        except Exception as e:
            self.logger.error(f"Failed to load topic classification model {self.model_name}: {e}")
            raise

    def classify_topic(self, text: str) -> str:
        """
        Classify the topic of the given text.

        Args:
            text: Text content to classify

        Returns:
            The most likely topic from the predefined list
        """
        if not self.classifier:
            self.logger.warning("Classifier not initialized, returning 'other'")
            return "other"

        if not text or not text.strip():
            return "other"

        try:
            # Truncate text to avoid model input limits
            text = text[:512]

            # Use the classifier with thread safety
            with self._lock:
                result = self.classifier(text, self.TOPICS)

            # Return the highest scoring topic
            return result['labels'][0]

        except Exception as e:
            self.logger.error(f"Error classifying topic for text: {e}")
            return "other"

    async def classify_topic_async(self, text: str) -> str:
        """
        Async wrapper for topic classification.

        Args:
            text: Text content to classify

        Returns:
            The most likely topic from the predefined list
        """
        # Run the synchronous classification in a thread pool
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.classify_topic, text)

    def classify_topics_batch(self, texts: List[str]) -> List[str]:
        """
        Classify topics for multiple texts.

        Args:
            texts: List of text contents to classify

        Returns:
            List of topics corresponding to each text
        """
        return [self.classify_topic(text) for text in texts]

    async def classify_topics_batch_async(self, texts: List[str]) -> List[str]:
        """
        Async batch classification of topics.

        Args:
            texts: List of text contents to classify

        Returns:
            List of topics corresponding to each text
        """
        # Process in batches to avoid overwhelming the model
        batch_size = 5
        results = []

        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[self.classify_topic_async(text) for text in batch]
            )
            results.extend(batch_results)

        return results

    def get_topic_confidence(self, text: str) -> dict:
        """
        Get classification confidence scores for all topics.

        Args:
            text: Text content to classify

        Returns:
            Dictionary with topics as keys and confidence scores as values
        """
        if not self.classifier:
            self.logger.warning("Classifier not initialized")
            return {}

        if not text or not text.strip():
            return {}

        try:
            # Truncate text to avoid model input limits
            text = text[:512]

            # Use the classifier with thread safety
            with self._lock:
                result = self.classifier(text, self.TOPICS)

            # Create confidence dictionary
            confidence_dict = {}
            for label, score in zip(result['labels'], result['scores']):
                confidence_dict[label] = float(score)

            return confidence_dict

        except Exception as e:
            self.logger.error(f"Error getting topic confidence for text: {e}")
            return {}

