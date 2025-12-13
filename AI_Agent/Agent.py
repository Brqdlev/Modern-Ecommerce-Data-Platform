from langchain_ollama import OllamaLLM
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent

llm = OllamaLLM(model="llama3:8b")

db = SQLDatabase.from_uri(
    "bigquery://my2ndproject-479707/Gold",
    include_tables=[
        "dim_stores",
        "dim_products",
        "dim_customers",
        "fact_orders",
        "fact_order_items",
        "fact_store_sales"
    ]
)

ASSISTANT_ROLE = """
You are a Business Intelligence AI Assistant working on the GOLD data warehouse.

Core Safety Rules:
- You may ONLY generate SELECT queries.
- You are READ-ONLY.
- You must NEVER modify data.
- The dataset contains analytical tables only.

RULES for OUTPUT:
- Explain the results politely.

RULES for Joining tales:
- If the question or answers is in the FACT tables/always do JOIN, to get the other data from other tables.
- If the question are like, "how many stores?" do not join, just get the answer in dim_stores, you do not need to join tables.
- If a FACT table is used and it contains a foreign key
  (example: store_id, product_id, customer_id),
  and the corresponding DIMENSION table exists,
  you MUST JOIN to the DIMENSION table.
- Use join if needed, if not do not use JOIN.

Explanation Rules:
- ALWAYS provide a short, simple natural-language explanation of the result.
- The explanation must summarize what the numbers mean in business terms.
"""




agent = create_sql_agent(
    llm=llm,
    db=db,
    verbose=True,
    agent_kwargs={"prefix": ASSISTANT_ROLE},
    handle_parsing_erros = True
)

print("\n✅ BigQuery AI Assistant is Ready\n")

while True:
    q = input("Ask your warehouse assistant > ")

    if q.lower() in ["exit", "quit"]:
        break

    try:
        answer = agent.run(q)
        print("\n✅ Assistant:\n", answer, "\n")
    except Exception as e:
        print("\n❌ Error:", e, "\n")
