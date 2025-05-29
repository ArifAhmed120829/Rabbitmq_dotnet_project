namespace SecondaryDbConsumer;

// Worker.cs
using System.Text;
using System.Text.Json;
using Dapper;
using Npgsql;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _config;

    public Worker(ILogger<Worker> logger, IConfiguration config)
    {
        _logger = logger;
        _config = config;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory()
        {
            HostName = _config["RabbitMQ:Host"],
            Port = _config.GetValue<int>("RabbitMQ:Port"),
            UserName = _config["RabbitMQ:Username"],
            Password = _config["RabbitMQ:Password"],
            DispatchConsumersAsync = true // Critical for async!
        };

        using var connection = factory.CreateConnection();
        using var channel = connection.CreateModel();

        // Match queue settings exactly with .netProject
        channel.QueueDeclare(
            queue: "employee_events",
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (model, ea) =>
        {
            try
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                
                _logger.LogInformation("Received message: {Message}", message);
                
                var eventData = JsonSerializer.Deserialize<EmployeeEvent>(message);
                
                _logger.LogInformation("Processing event: {EventType} for Employee: {EmpId}", 
                    eventData.EventType, eventData.Data?.EmpId);

                await SyncToSecondaryDb(eventData);
                channel.BasicAck(ea.DeliveryTag, false); // Manual ACK
                
                _logger.LogInformation("Successfully processed event: {EventType}", eventData.EventType);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Message processing failed");
                channel.BasicNack(ea.DeliveryTag, false, true); // Requeue
            }
        };

        channel.BasicConsume(
            queue: "employee_events",
            autoAck: false, // Manual acknowledgment
            consumer: consumer);

        _logger.LogInformation("Worker started, waiting for messages...");

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }
    }

    private async Task SyncToSecondaryDb(EmployeeEvent @event)
    {
        var pgConnString = _config.GetConnectionString("SecondaryDb");
        using var db = new NpgsqlConnection(pgConnString);

        try
        {
            await db.OpenAsync();
            
            switch (@event.EventType)
            {
                case "EmployeeCreated":
                    _logger.LogInformation("Creating employee with ID: {EmpId}", @event.Data.EmpId);
                    
                    await db.ExecuteAsync(
                        @"INSERT INTO ""Employees"" (
                            ""EmpId"", ""EmpName"", ""EmpCode"", ""Gender"", ""ComId"", ""DeptId"", 
                            ""DesigId"", ""ShiftId"", ""Gross"", ""basic"",
                            ""HRent"", ""Medical"", ""Others"", ""dtJoin""
                          ) VALUES (
                            @EmpId, @EmpName, @EmpCode, @Gender, @ComId, @DeptId,
                            @DesigId, @ShiftId, @Gross, @basic,
                            @HRent, @Medical, @Others, @dtJoin
                          ) ON CONFLICT (""EmpId"") DO UPDATE 
                          SET ""EmpName"" = EXCLUDED.""EmpName"",
                              ""EmpCode"" = EXCLUDED.""EmpCode"",
                              ""Gender"" = EXCLUDED.""Gender"",
                              ""ComId"" = EXCLUDED.""ComId"",
                              ""DeptId"" = EXCLUDED.""DeptId"",
                              ""DesigId"" = EXCLUDED.""DesigId"",
                              ""ShiftId"" = EXCLUDED.""ShiftId"",
                              ""Gross"" = EXCLUDED.""Gross"",
                              ""basic"" = EXCLUDED.""basic"",
                              ""HRent"" = EXCLUDED.""HRent"",
                              ""Medical"" = EXCLUDED.""Medical"",
                              ""Others"" = EXCLUDED.""Others"",
                              ""dtJoin"" = EXCLUDED.""dtJoin""",
                        new {
                            @event.Data.EmpId,
                            @event.Data.EmpName,
                            @event.Data.EmpCode,
                            @event.Data.Gender,
                            @event.Data.ComId,
                            @event.Data.DeptId,
                            @event.Data.DesigId,
                            @event.Data.ShiftId,
                            @event.Data.Gross,
                            @event.Data.basic,
                            @event.Data.HRent,
                            @event.Data.Medical,
                            @event.Data.Others,
                            @event.Data.dtJoin
                        });
                    break;

                case "EmployeeUpdated":
                    _logger.LogInformation("Updating employee with ID: {EmpId}", @event.Data.EmpId);
                    
                    var rowsAffected = await db.ExecuteAsync(
                        @"UPDATE ""Employees"" 
                          SET ""EmpName"" = @EmpName,
                              ""EmpCode"" = @EmpCode,
                              ""Gender"" = @Gender,
                              ""ComId"" = @ComId,
                              ""DeptId"" = @DeptId,
                              ""DesigId"" = @DesigId,
                              ""ShiftId"" = @ShiftId,
                              ""Gross"" = @Gross,
                              ""basic"" = @basic,
                              ""HRent"" = @HRent,
                              ""Medical"" = @Medical,
                              ""Others"" = @Others,
                              ""dtJoin"" = @dtJoin
                          WHERE ""EmpId"" = @EmpId",
                        new {
                            @event.Data.EmpId,
                            @event.Data.EmpName,
                            @event.Data.EmpCode,
                            @event.Data.Gender,
                            @event.Data.ComId,
                            @event.Data.DeptId,
                            @event.Data.DesigId,
                            @event.Data.ShiftId,
                            @event.Data.Gross,
                            @event.Data.basic,
                            @event.Data.HRent,
                            @event.Data.Medical,
                            @event.Data.Others,
                            @event.Data.dtJoin
                        });
                    
                    if (rowsAffected == 0)
                    {
                        _logger.LogWarning("No employee found with ID {EmpId} to update", @event.Data.EmpId);
                    }
                    break;

                case "EmployeeDeleted":
                    _logger.LogInformation("Deleting employee with ID: {EmpId}", @event.Data.EmpId);
                    
                    var deletedRows = await db.ExecuteAsync(
                        @"DELETE FROM ""Employees"" WHERE ""EmpId"" = @EmpId",
                        new { EmpId = @event.Data.EmpId });
                    
                    if (deletedRows == 0)
                    {
                        _logger.LogWarning("No employee found with ID {EmpId} to delete", @event.Data.EmpId);
                    }
                    else
                    {
                        _logger.LogInformation("Successfully deleted employee with ID: {EmpId}", @event.Data.EmpId);
                    }
                    break;

                default:
                    _logger.LogWarning("Unknown event type: {EventType}", @event.EventType);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Database operation failed for event type: {EventType}", @event.EventType);
            throw; // Re-throw to trigger message requeue
        }
    }
}

// SharedModels/EmployeeEvent.cs
public class EmployeeEvent
{
    public string EventType { get; set; } // "EmployeeCreated", "EmployeeUpdated", "EmployeeDeleted"
    public Employee Data { get; set; }
    public DateTime Timestamp { get; set; }
}

// Employee model for the secondary database consumer
public class Employee
{
    public int EmpId { get; set; }
    public int ComId { get; set; }
    public int ShiftId { get; set; }
    public int DeptId { get; set; }
    public int DesigId { get; set; }
    public int EmpCode { get; set; }
    public string EmpName { get; set; }
    public string Gender { get; set; }
    public int Gross { get; set; }
    public int basic { get; set; }
    public int HRent { get; set; }
    public int Medical { get; set; }
    public int Others { get; set; }
    public DateTime? dtJoin { get; set; }
}