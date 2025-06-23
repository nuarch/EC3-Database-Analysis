
# chatgpt_config.py
"""
ChatGPT API configuration file containing API credentials and settings.
This file should be added to .gitignore to prevent checking API keys into git.
"""

# ChatGPT API Configuration
CHATGPT_CONFIG = {
    'api_key': '###',  # Replace with your actual OpenAI API key
    'base_url': 'https://api.openai.com/v1',  # OpenAI API endpoint
    'model': 'gpt-4o',  # Model to use (gpt-4, gpt-3.5-turbo, etc.)
    'timeout': 60,  # Request timeout in seconds
    'max_retries': 3,  # Maximum number of retry attempts for failed requests
    'max_tokens': 2000,  # Maximum tokens for response
    'temperature': 0.1  # Temperature for response consistency
}
