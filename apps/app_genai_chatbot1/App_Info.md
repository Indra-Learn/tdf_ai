## Reference:

1. [Youtube Video](https://www.youtube.com/watch?v=E4l91XKQSgw)
2. [Learn GenAI](..\app1\Learn_GenAI.md) 


## App Info:

1. Download & Install [ollama](https://ollama.com/download)
2. Install python packages: langchain, langchain-ollama, langchain-chroma, pandas
3. open cmd externally and execute below 
    ```shell
    ollama pull llama3.2
    ollama pull mxbai-embed-large
    ollama list
    ```
4. extute from project folder <u>tdf_ai</u>: `python ./apps/app2/main.py`
5. question: "What is the overall updates of India Stock Market today?"


## Extras:

1. ollama models: https://ollama.com/library
2. use `llama3.2` for LLM and use `mxbai-embed-large` as embedding model to embed the documents and add into vector store.
