from abc import ABC, abstractmethod
import logging
from bs4 import BeautifulSoup

class ArticleProcessor(ABC):
    def _get_title(self, article_soup: BeautifulSoup) -> str:
        return article_soup.find("main").find("h1").get_text(strip=True)

    def get_title(self, article_soup: BeautifulSoup) -> str:
        try:
            article_title = self._get_title(article_soup)
        except:
            article_title = None
        return article_title

    @abstractmethod
    def _get_text(self, article_soup: BeautifulSoup) -> str:
        pass
    def get_text(self, article_soup: BeautifulSoup) -> str:
        try:
            article_text = self._get_text(article_soup)
        except Exception as e:
            logging.warning("Failed to get article text.")
            logging.warning(f"Error: {e}")
            article_text = None
        return article_text
    def process(self, article_soup: BeautifulSoup) -> tuple[str,str]:
        title, text = self.get_title(article_soup), self.get_text(article_soup)
        return title, text