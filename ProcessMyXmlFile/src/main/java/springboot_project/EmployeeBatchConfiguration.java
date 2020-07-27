package springboot_project;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.oxm.xstream.XStreamMarshaller;


import spring.model.Employee;

@Configuration
@EnableBatchProcessing
public class EmployeeBatchConfiguration {

	@Autowired
	public JobBuilderFactory  jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory  stepBuilderFactory;
	
	@Autowired
	public DataSource dataSource;
	
	@Bean
	public DataSource datasource(){
		final DriverManagerDataSource ds=new DriverManagerDataSource();
		//ds.setDriverClassName("com.mysql.jdbc.Driver");
		ds.setUrl("jdbc:mysql://localhost:3306/springboot");
		ds.setUsername("root");
		ds.setPassword("root11");
		
		return ds;
	}
	
	
	  @Bean public StaxEventItemReader<Employee> reader(){
	  StaxEventItemReader<Employee> staxReader=new StaxEventItemReader<Employee>();
	  staxReader.setResource(new ClassPathResource("employee.xml"));
	  staxReader.setFragmentRootElementName("employee");
	  
	  
	  Map<String, String> aliases = new HashMap<String, String>();
	  aliases.put("employee", "spring.model.Employee");
	  
	  XStreamMarshaller xStreamMarshaller = new XStreamMarshaller();
	  xStreamMarshaller.setAliases(aliases);
	  
	  staxReader.setUnmarshaller(xStreamMarshaller);
	  
	  return staxReader; }
	 
	  
	@Bean
	public JdbcBatchItemWriter <Employee> writer(){
		JdbcBatchItemWriter<Employee> writer=new JdbcBatchItemWriter<Employee>();
		writer.setDataSource(dataSource);
		writer.setSql("replace into employee(id,name) values(?, ?)");
		writer.setItemPreparedStatementSetter(new EmployeePreparedStmSetter());
		
		return writer;
	}
	
	 private class EmployeePreparedStmSetter implements ItemPreparedStatementSetter<Employee>{

		  @Override
		  public void setValues(Employee emp, PreparedStatement ps) throws SQLException {
		   ps.setInt(1, emp.getId());
		   ps.setString(2, emp.getName());
		  }
		  
	}
	 
	 @Bean
	 public Step step1() {
	  return stepBuilderFactory.get("step1")
	    .<Employee, Employee> chunk(10)
	    .reader(reader())
	    .writer(writer())
	    .build();
	 }
	 
	 @Bean
	 public Job importEmployeeJob() {
	  return jobBuilderFactory.get("importUserJob")
	    .incrementer(new RunIdIncrementer())
	    .flow(step1())
	    .end()
	    .build();
	    
	 }
	
	
}
