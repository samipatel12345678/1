from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
import datetime
from config.supabase import supabase

app = FastAPI()

scheduler = BackgroundScheduler()

def update_search_insights_cron():
    try:
        today = datetime.date.today()
        start_date = today - datetime.timedelta(days=1)
        end_date = today - datetime.timedelta(days=1)

        response = supabase.from_("search_click").select("*") \
            .gte("search_date", start_date) \
            .lte("search_date", end_date) \
            .execute()

        raw_data = response.data

        if not raw_data:
            return

        daily_ctr = {}
        for record in raw_data:
            date = record["search_date"]
            ctr = record["click_through_rate"]
            daily_ctr.setdefault(date, []).append(ctr)

        average_ctr_per_day = {date: sum(ctr_list) / len(ctr_list) for date, ctr_list in daily_ctr.items()}
        overall_average_ctr = sum(average_ctr_per_day.values()) / len(average_ctr_per_day)

        query_ctr = {}
        for record in raw_data:
            query = record["search_query"]
            ctr = record["click_through_rate"]
            query_ctr.setdefault(query, []).append(ctr)

        average_query_ctr = {query: sum(ctr_list) / len(ctr_list) for query, ctr_list in query_ctr.items()}
        top_queries = sorted(average_query_ctr, key=average_query_ctr.get, reverse=True)[:5]

        low_performance_queries = []
        query_metrics = {}
        for record in raw_data:
            query = record["search_query"]
            ctr = record["clicks"]
            impression = record["impressions"]
            if query not in query_metrics:
                query_metrics[query] = {"impressions": 0, "total_clicks": 0}
            query_metrics[query]["impressions"] += impression
            query_metrics[query]["total_clicks"] += ctr

        for query, metrics in query_metrics.items():
            impressions = metrics["impressions"]
            total_clicks = metrics["total_clicks"]
            if impressions > 500 and total_clicks <= 200:
                low_performance_queries.append(query)

        insight_date = today.isoformat()
        insights_data = [{
            "insight_date": insight_date,
            "average_ctr": overall_average_ctr,
            "top_queries": top_queries,
            "low_performance_queries": low_performance_queries,
        }]
        supabase.from_("search_insights").insert(insights_data).execute()

    except Exception as e:
        print(f"Error updating search insights: {str(e)}")

def start_scheduler():
    scheduler.add_job(update_search_insights_cron, "cron", hour=9, minute=0)
    scheduler.start()

def shutdown_scheduler():
    scheduler.shutdown()

app.add_event_handler("startup", start_scheduler)
app.add_event_handler("shutdown", shutdown_scheduler)
