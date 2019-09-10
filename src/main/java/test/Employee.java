package test;

import java.io.Serializable;

public class Employee implements Serializable {
  private static final long serialVersionUID = 2522349042622085978L;

  private int id;
  private int relation;
  private String name;
  private int age;

  /**
   * All-args constructor.
   * @param id The new value for id
   * @param relation The new value for relation
   * @param name The new value for name
   * @param age The new value for age
   */
  public Employee(int id, int relation, String name, int age) {
    this.id = id;
    this.relation = relation;
    this.name = name;
    this.age = age;
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public int getId() {
    return id;
  }

  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(int value) {
    this.id = value;
  }

  /**
   * Gets the value of the 'relation' field.
   * @return The value of the 'relation' field.
   */
  public int getRelation() {
    return relation;
  }

  /**
   * Sets the value of the 'relation' field.
   * @param value the value to set.
   */
  public void setRelation(int value) {
    this.relation = value;
  }

  /**
   * Gets the value of the 'name' field.
   * @return The value of the 'name' field.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the value of the 'name' field.
   * @param value the value to set.
   */
  public void setName(String value) {
    this.name = value;
  }

  /**
   * Gets the value of the 'age' field.
   * @return The value of the 'age' field.
   */
  public Integer getAge() {
    return age;
  }

  /**
   * Sets the value of the 'age' field.
   * @param value the value to set.
   */
  public void setAge(int value) {
    this.age = value;
  }

}
