"""
Here are some relevant indian stock market updates: 
    1. Today's india stock market status: {daily_market_updates}
    2. Today's Large Deals are: {large_deals}
    3. Today's FII's and DII's total trade activity: {fii_dii_trades}
"""

from langchain_ollama import OllamaLLM
from langchain_core.prompts import ChatPromptTemplate
from vector import retriever

model = OllamaLLM(model="llama3.2")

template = """
    You are an expert in answering question about indian stock market and investment strategies for the company.

    Here are some relevant indian stock market updates: 
    1. Today's Large Deals are: {large_deals}

    Here is the question to answer: {question}
"""

prompt = ChatPromptTemplate.from_template(template) 
chain = prompt | model 

while True:
    print("\n\n-------------------------------------------------")
    question = input("Ask your question (q to Quit): ")
    print("\n\n")
    if question == "q":
        break
    
    large_deals = retriever.invoke(question)
    result = chain.invoke({"large_deals": large_deals,
                           "question": question})
    print(result)