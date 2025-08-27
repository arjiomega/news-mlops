from article_processor.article_processors import (
    CNNArticleProcessor,
    GizmodoArticleProcessor,
    Google9to5ArticleProcessor,
    AlJazeeraEnglishArticleProcessor,
    BBCNewsArticleProcessor,
    JalopnikArticleProcessor,
    MacRumorsArticleProcessor,
    NBCNewsArticleProcessor,
    NintendoLifeArticleProcessor,
    PhoronixArticleProcessor,
    PolygonArticleProcessor,
    TheVergeArticleProcessor
)
from article_processor.base import ArticleProcessor

import logging
logger = logging.getLogger(__name__)

def article_processor_loader(article_processor_name: str) -> ArticleProcessor:
    article_processors = {
        "9to5google": Google9to5ArticleProcessor(),
        "al-jazeera-english": AlJazeeraEnglishArticleProcessor(),
        "bbc-news": BBCNewsArticleProcessor(),
        "cnn": CNNArticleProcessor(),
        "gizmodo": GizmodoArticleProcessor(),
        "jalopnik": JalopnikArticleProcessor(),
        "macrumors": MacRumorsArticleProcessor(),
        "nbc-news": NBCNewsArticleProcessor(),
        "nintendolife": NintendoLifeArticleProcessor(),
        "phoronix": PhoronixArticleProcessor(),
        "polygon": PolygonArticleProcessor(),
        "theverge": TheVergeArticleProcessor(),
    }
    try:
        return article_processors[article_processor_name]
    except Exception as e:
        logger.error(f"Invalid article processor name. Available are the following [{', '.join(article_processors.keys())}]") 