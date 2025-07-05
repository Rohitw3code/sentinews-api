# analysis/sentiment_analyzer.py

import os
from dotenv import load_dotenv
from typing import List, Literal, Any

from langchain_core.prompts import ChatPromptTemplate
from pydantic.v1 import BaseModel, Field, ValidationError

from langchain_openai import ChatOpenAI
from langchain_groq import ChatGroq
from langchain_community.callbacks import get_openai_callback
from langchain_core.callbacks import BaseCallbackHandler

# --- Default Configuration ---
# These values are used if no specific configuration is passed during initialization.
load_dotenv()
DEFAULT_LLM_PROVIDER = 'openai'
DEFAULT_OPENAI_MODEL_NAME = 'gpt-4o-mini'
DEFAULT_GROQ_MODEL_NAME = 'llama3-8b-8192'

# --- Pydantic Data Structures ---
# Defines the expected JSON output structure for the AI model.
class EntitySentiment(BaseModel):
    """A data model for the dual sentiment towards a single, validated entity."""
    entity_name: str = Field(
        description="The full, official name of the company or cryptocurrency, resolved from any abbreviations (e.g., 'IBM' becomes 'International Business Machines')."
    )
    entity_type: Literal["company", "crypto"] = Field(
        description="The type of the entity."
    )
    financial_sentiment: Literal["positive", "negative", "neutral"] = Field(
        description="Sentiment based ONLY on financial performance like stock prices, earnings, and market data."
    )
    overall_sentiment: Literal["positive", "negative", "neutral"] = Field(
        description="Sentiment based on general news like company decisions, product launches, partnerships, or legal issues."
    )
    reasoning: str = Field(
        description="Brief justification for both sentiment classifications, explaining the key factors from the text."
    )

class TextAnalysis(BaseModel):
    """A data model to hold the entity sentiment analysis of a given text."""
    entities: List[EntitySentiment] = Field(
        description="A list of valid entities. This list MUST be empty if no valid entities are found."
    )

# --- Groq Callback for Token Tracking ---
class GroqTokenUsageCallback(BaseCallbackHandler):
    """Callback handler to capture token usage from Groq, as it's not natively supported like OpenAI's."""
    def __init__(self):
        self.usage = {}

    def on_llm_end(self, response, **kwargs: Any) -> Any:
        """Run when LLM ends running and capture the token usage from the response metadata."""
        if response.llm_output and 'token_usage' in response.llm_output:
            self.usage = response.llm_output['token_usage']

# --- Main Analyzer Class ---
class SentimentAnalyzer:
    """
    A configurable class to perform sentiment analysis using different LLM providers.
    """
    def __init__(self, provider=None, model_name=None, openai_api_key=None, groq_api_key=None):
        """
        Initializes the analyzer with specific or default configurations.
        Allows for API keys and model details to be passed directly, bypassing .env files if needed.
        """
        self.provider = provider or DEFAULT_LLM_PROVIDER
        
        if model_name:
            self.model_name = model_name
        else:
            self.model_name = DEFAULT_OPENAI_MODEL_NAME if self.provider == 'openai' else DEFAULT_GROQ_MODEL_NAME

        self.openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.groq_api_key = groq_api_key or os.getenv("GROQ_API_KEY")

        self.chain = self._initialize_chain()

    def _initialize_chain(self):
        """Initializes and returns the appropriate language model and LangChain chain."""
        llm = None
        if self.provider == 'openai':
            print(f"Initializing OpenAI model: {self.model_name}")
            if not self.openai_api_key:
                raise ValueError("OpenAI API key not found. Please provide it in the API call or set it in the .env file.")
            llm = ChatOpenAI(model_name=self.model_name, temperature=0, api_key=self.openai_api_key)
        
        elif self.provider == 'groq':
            print(f"Initializing Groq model: {self.model_name}")
            if not self.groq_api_key:
                raise ValueError("Groq API key not found. Please provide it in the API call or set it in the .env file.")
            llm = ChatGroq(model_name=self.model_name, temperature=0, api_key=self.groq_api_key)
        
        else:
            raise ValueError(f"Unsupported LLM provider: {self.provider}. Please choose 'openai' or 'groq'.")

        structured_llm = llm.with_structured_output(TextAnalysis)
        
        system_prompt = """
        You are a highly precise financial analyst. Your task is to extract **only legitimate companies and cryptocurrencies** from the provided text and analyze them from two different perspectives: **financial sentiment** and **overall sentiment**.
        
        **CRITICAL RULES:**
        1.  **RESOLVE FULL ENTITY NAME:** You MUST return the full, official name of the entity (e.g., "IBM" becomes "International Business Machines").
        2.  **DO NOT EXTRACT LOCATIONS:** Ignore countries, cities, etc.
        3.  **EMPTY LIST IS VALID:** If you find no valid entities, return an empty list.
        
        **RULES FOR DUAL SENTIMENT ANALYSIS:**
        1.  **Financial Sentiment:** Strictly about quantitative performance (stocks, earnings).
        2.  **Overall Sentiment:** About qualitative, operational news (products, partnerships).
        
        **OUTPUT FORMAT:**
        For each valid entity, provide its resolved official name, type, financial sentiment, overall sentiment, and a brief reasoning. **It is critical that every entity object in your JSON output contains all required fields.**
        """
        prompt = ChatPromptTemplate.from_messages([
            ("system", system_prompt),
            ("human", "{text}")
        ])
        return prompt | structured_llm

    def analyze_text_for_sentiment(self, text: str):
        """Analyzes text using the configured chain, with retry logic for robustness."""
        if not self.chain:
            print("Chain not initialized.")
            return [], {}
            
        print(f"\nAnalyzing article for sentiment with dual analysis using {self.provider} ({self.model_name})...")
        
        MAX_RETRIES = 3
        for attempt in range(MAX_RETRIES):
            try:
                if self.provider == 'openai':
                    with get_openai_callback() as cb:
                        response = self.chain.invoke({"text": text})
                        usage_stats = {"total_tokens": cb.total_tokens, "prompt_tokens": cb.prompt_tokens, "completion_tokens": cb.completion_tokens, "total_cost_usd": cb.total_cost}
                        print(f"OpenAI Usage: {usage_stats['total_tokens']} tokens. Cost: ${usage_stats['total_cost_usd']:.6f} USD")
                        return response.entities, usage_stats

                elif self.provider == 'groq':
                    token_callback = GroqTokenUsageCallback()
                    response = self.chain.invoke({"text": text}, config={"callbacks": [token_callback]})
                    token_usage = token_callback.usage
                    # Cost calculation for Groq models can be added here based on their pricing page.
                    usage_stats = {"total_tokens": token_usage.get('total_tokens', 0), "prompt_tokens": token_usage.get('prompt_tokens', 0), "completion_tokens": token_usage.get('completion_tokens', 0), "total_cost_usd": 0.0}
                    print(f"Groq Usage: {usage_stats['total_tokens']} tokens.")
                    return response.entities, usage_stats
            
            except ValidationError as e:
                print(f"Validation error (Attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt >= MAX_RETRIES - 1:
                    return [], {}
                print("Retrying...")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                return [], {}
        return [], {}
