# Real-Time Election Voting System  

A real-time election voting system designed to simulate and visualize the voting process with real-time data streaming and analytics. This project showcases the integration of various technologies to create a dynamic and interactive voting experience.  

## 🛠️ Technologies Used  
- **PostgreSQL**: For storing voter and candidate demographics generated via an external API.  
- **Confluent Kafka**: To stream votes in real-time and enable high-throughput data processing.  
- **Apache Spark**: For efficient aggregation and processing of streamed voting data.  
- **Streamlit**: To build an interactive dashboard for live visualization of vote counts.  
- **Docker**: For containerizing all components, ensuring seamless deployment and scalability.  

## 📚 Features  
- **Voter and Candidate Data Integration**:  
  - External APIs were used to generate voter and candidate demographics, which were stored in a PostgreSQL database for easy management.  

- **Real-Time Vote Streaming**:  
  - Kafka was employed to stream vote data in real-time, ensuring high availability and low latency.  

- **Efficient Data Processing**:  
  - Spark processed the streamed vote data to provide aggregated insights and analytics quickly and efficiently.  

- **Interactive Dashboard**:  
  - A Streamlit-powered dashboard displayed live vote counts, visualizations, and updates, offering a clear overview of the election progress.  

- **Containerized Architecture**:  
  - All components of the system were containerized using Docker, allowing for easy deployment and horizontal scalability.  

## 🚀 Highlights  
This project demonstrates the power of real-time data streaming and analytics by simulating an election scenario. It integrates robust data engineering pipelines, real-time processing, and user-friendly visualizations to provide a comprehensive solution.  

## 📷 Screenshots  
*(Add screenshots or GIFs of your Streamlit dashboard here to give users a visual understanding of the project.)*  

## 🔧 How to Run  
1. **Clone the Repository**:  
   ```bash  
   git clone https://github.com/rajsalvi744/realtime-voting.git
   cd realtime-voting 
2. Set Up Docker:
   Ensure Docker is installed on your machine, then start the containers:
   ```bash
   docker-compose up  
3. Access the Dashboard: Open your browser and navigate to http://localhost:8501 to view the interactive dashboard.
4. Simulate Voting: Use the scripts provided in the repository to simulate voter and candidate data generation and real-time vote streaming.
