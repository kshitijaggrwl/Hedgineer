from pydantic import BaseModel, Field
from datetime import date
from typing import Optional

class BuildIndexRequest(BaseModel):
    start_date: date = Field(..., description="Start date for index construction (YYYY-MM-DD)")
    end_date: Optional[date] = Field(None, description="Optional end date. If not provided, start_date is used.")
    
class DateRange(BaseModel):
    start_date: date
    end_date: date

class CompositionQuery(BaseModel):
    date: date
