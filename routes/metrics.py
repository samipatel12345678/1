from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import datetime
from config.supabase import supabase  # Importing the Supabase client

router = APIRouter()

class MetricsRequest(BaseModel):
    start_date: datetime.date
    end_date: datetime.date

class MetricsResponse(BaseModel):
    insight_date: str  # Change to string to handle serialization
    average_ctr: float
    top_queries: list[str]
    low_performance_queries: list[str]

@router.post("/update-insights", response_model=MetricsResponse)
async def update_search_insights(request: MetricsRequest):
    try:
        # Log the request dates to verify correctness
        print("Request Start Date:", request.start_date)
        print("Request End Date:", request.end_date)

        # Step 1: Fetch raw data for the date range
        response = supabase.from_("search_click").select("*") \
            .gte("search_date", request.start_date) \
            .lte("search_date", request.end_date) \
            .execute()

        # Log the full response to check for errors or missing data
        print("Full response:", response)

        # Extract data
        raw_data = response.data
        print("This is raw_data:", raw_data)

        # Step 2: Perform calculations if raw_data exists
        if not raw_data:
            print("No data returned for the given date range.")
            return MetricsResponse(
                insight_date=datetime.date.today().isoformat(),  # Convert date to string
                average_ctr=0.0,
                top_queries=[],
                low_performance_queries=[]
            )

        # Calculate daily average CTR
        daily_ctr = {}
        for record in raw_data:
            date = record["search_date"]
            ctr = record["click_through_rate"]
            daily_ctr.setdefault(date, []).append(ctr)

        average_ctr_per_day = {date: sum(ctr_list) / len(ctr_list) for date, ctr_list in daily_ctr.items()}
        overall_average_ctr = sum(average_ctr_per_day.values()) / len(average_ctr_per_day)
        print("This is daily ctr: ", daily_ctr)

        # Identify top 5 queries by CTR
        query_ctr = {}
        for record in raw_data:
            query = record["search_query"]
            ctr = record["click_through_rate"]
            query_ctr.setdefault(query, []).append(ctr)

        average_query_ctr = {query: sum(ctr_list) / len(ctr_list) for query, ctr_list in query_ctr.items()}
        top_queries = sorted(average_query_ctr, key=average_query_ctr.get, reverse=True)[:5]
        print("This is query ctr: ", query_ctr)

        low_performance_queries = []
        # Create a dictionary to store impressions and clicks for each query
        query_metrics = {}

        # Step through raw_data to accumulate impressions and clicks for each query
        for record in raw_data:
            query = record["search_query"]
            ctr = record["clicks"]
            impression = record["impressions"]  # Assuming 'impression' is the field name for impressions
            
            # Initialize or accumulate impressions and clicks for the query
            if query not in query_metrics:
                query_metrics[query] = {"impressions": 0, "total_clicks": 0}
            
            # Accumulate impressions and clicks
            query_metrics[query]["impressions"] += impression  # Use the actual impression value
            query_metrics[query]["total_clicks"] += ctr

        # Now, loop through the query_metrics to find low performance queries
        for query, metrics in query_metrics.items():
            impressions = metrics["impressions"]
            total_clicks = metrics["total_clicks"]
            
            # Log the query's impressions and total clicks
            print(f"Query: {query}, Impressions: {impressions}, Total Clicks: {total_clicks}")

            # Low performance query criteria: high impressions and low total clicks
            if impressions > 500 and total_clicks <= 200:
                low_performance_queries.append(query)


        print("Low performance queries:", low_performance_queries)  # Log the final result

        # Step 3: Insert metrics into `search_insights`
        insight_date = datetime.date.today().isoformat()  # Convert date to string for database insertion
        insights_data = [{
            "insight_date": insight_date,
            "average_ctr": overall_average_ctr,
            "top_queries": top_queries,
            "low_performance_queries": low_performance_queries,
        }]
        insert_response = supabase.from_("search_insights").insert(insights_data).execute()

        # Return response with date as string
        return MetricsResponse(
            insight_date=insight_date,  # Return as string
            average_ctr=overall_average_ctr,
            top_queries=top_queries,
            low_performance_queries=low_performance_queries
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating search insights: {str(e)}")
