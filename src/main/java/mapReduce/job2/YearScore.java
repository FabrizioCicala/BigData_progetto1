package mapReduce.job2;


public class YearScore {

	private int year;
	private int score;



	public YearScore(int y, int s) {
		this.year = y;
		this.score = s;  
	}


	public int getScore() {
		return this.score;
	}


	public void setScore(int score) {
		this.score = score;
	}


	public int getYear() {
		return this.year;
	}


	public void setYear(int year) {
		this.year = year;
	}


	@Override
	public String toString() {
		return "[score=" + this.score + ", year=" + this.year + "]";
	}




	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + score;
		result = prime * result + year;
		return result;
		
		//return this.score + this.year;
	}


    @Override
    public boolean equals(Object o) {
        if (o instanceof YearScore) {
        	YearScore yearScore = (YearScore) o;
            return (this.score == yearScore.getScore())
                    && ( this.year == yearScore.getYear()) ;
        }
        return false;
    }
    
}
