import streamlit as st
from query_and_retrieve import query_and_answer

# Streamlit interface
st.title("Document Query Interface")

query = st.text_input("Enter your query:", "")

if st.button("Search"):
    if query:
        st.write(f"Searching for: {query}")
        results = query_and_answer(query)
        for section_id, answer in results:
            st.write(f"**Section ID:** {section_id}")
            st.write(f"**Answer:** {answer}")
            st.write("---")
    else:
        st.write("Please enter a query.")
