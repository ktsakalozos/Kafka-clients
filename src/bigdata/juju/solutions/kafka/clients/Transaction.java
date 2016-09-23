package bigdata.juju.solutions.kafka.clients;

import java.io.Serializable;

public class Transaction implements Serializable {
	Double amount;
    public Double getamount() {
		return amount;
	}

	public void setamount(Double amount) {
		this.amount = amount;
	}

	public String getentity() {
		return entity;
	}

	public void setentity(String entity) {
		this.entity = entity;
	}


	String entity;

    public Transaction() {}

    public Transaction(String entity, Double amount)
    {
        this.amount=amount;
        this.entity=entity;
    }


    public String toString(){
        return "Transaction'{'entity="+entity+"{0}, amount="+amount+"'}'";
    }

}
