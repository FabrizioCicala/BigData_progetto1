package mapReduce.job2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class YearScore implements WritableComparable<YearScore> {

	private IntWritable year;
	private IntWritable score;

	/** Constructor
     *  init class variables from IntWritable parameters
	 **/
    YearScore (IntWritable year, IntWritable score){
        this.year = year;
        this.score = score;
    }

    /** Constructor
     *  init class variables from int parameters
     **/
	YearScore(int year, int score) {
		this.year = new IntWritable(year);
		this.score = new IntWritable(score);
	}

	/** getter methods **/
    IntWritable getYear() { return this.year; }

    IntWritable getScore() { return this.score;	}

    /** setter methods **/
    void setYear(int year) { this.year.set(year); }
    void setYear(IntWritable year) { this.year = year; }

	public void setScore(int score) { this.score.set(score); }
    public void setScore(IntWritable score) { this.score = score; }

    /** other methods **/
	@Override
	public String toString() {
		return "[score=" + this.score.get() + ", year=" + this.year.get() + "]";
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + score.get();
		result = prime * result + year.get();
		return result;
		
		//return this.score + this.year;
	}


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof YearScore) {
        	YearScore yearScore = (YearScore) obj;
            return (this.score.get() == yearScore.getScore().get())
                    && ( this.year.get() == yearScore.getYear().get()) ;
        }
        return false;
    }


    /** writable methods implementation **/
	@Override
	public int compareTo(YearScore yearScore) {
		return equals(yearScore) ? 1 : -1 ;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.year.toString());
        dataOutput.writeUTF(this.score.toString());
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
        year = new IntWritable(Integer.parseInt(dataInput.readUTF()));
        score = new IntWritable(Integer.parseInt(dataInput.readUTF()));
    }
}
