import java.io.Serializable;

public class Word implements Serializable {
    private String word;
    private Integer count;

    Word(String word, Integer count) {
        this.count = count;
        this.word = word;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
