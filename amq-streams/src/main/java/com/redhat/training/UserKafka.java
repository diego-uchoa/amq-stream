package com.redhat.training;

public class UserKafka {
    private int id;
    private String name, email; 

    UserKafka(int id, String name, String email){
        this.id = id;
        this.name = name;
        this.email = email;
    }

    public int getId(){return id;}
    public String getName(){return name;}
    public String getEmail(){return email;}

    @Override
    public String toString() {
        return "UserKafka{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", email='" + email + '\'' +
                '}';
    }
}
