using System.Collections.Concurrent;
using System.Text;

using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;
using Azure.Storage.Blobs;

using Microsoft.Extensions.Logging;

var storageConnectionString = "<< CONNECTION STRING FOR THE STORAGE ACCOUNT >>";
var blobContainerName = "<< NAME OF THE BLOB CONTAINER >>";

var eventHubsConnectionString = "<< CONNECTION STRING FOR THE EVENT HUBS NAMESPACE >>";
var eventHubName = "<< NAME OF THE EVENT HUB >>";
var consumerGroup = "<< NAME OF THE EVENT HUB CONSUMER GROUP >>";

var storageClient = new BlobContainerClient(
    storageConnectionString,
    blobContainerName);

var processor = new EventProcessorClient(
    storageClient,
    consumerGroup,
    eventHubsConnectionString,
    eventHubName);

using var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
var logger = loggerFactory.CreateLogger("EventHubConsumer");

var partitionEventCount = new ConcurrentDictionary<string, int>();

using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(60));

processor.ProcessEventAsync += processEventHandler;
processor.ProcessErrorAsync += processErrorHandler;

try
{
    await processor.StartProcessingAsync(cts.Token);
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException) when (cts.IsCancellationRequested)
{
    // This is expected if the cancellation token is
    // signaled.
}
finally
{
    // This may take up to the length of time defined
    // as part of the configured TryTimeout of the processor;
    // by default, this is 60 seconds.

    await processor.StopProcessingAsync();

    processor.ProcessEventAsync -= processEventHandler;
    processor.ProcessErrorAsync -= processErrorHandler;
}

async Task processEventHandler(ProcessEventArgs args)
{
    try
    {
        // If the cancellation token is signaled, then the
        // processor has been asked to stop.  It will invoke
        // this handler with any events that were in flight;
        // these will not be lost if not processed.
        //
        // It is up to the handler to decide whether to take
        // action to process the event or to cancel immediately.

        if (args.CancellationToken.IsCancellationRequested)
        {
            return;
        }

        var partition = args.Partition.PartitionId;
        var eventBody = args.Data.EventBody.ToArray();
        logger.LogInformation("Event from partition {partition} with length {eventBodyLength}.", partition, eventBody.Length);
        var message = Encoding.UTF8.GetString(eventBody);
        logger.LogInformation(message);

        var eventsSinceLastCheckpoint = partitionEventCount.AddOrUpdate(
            key: partition,
            addValue: 1,
            updateValueFactory: (_, currentCount) => currentCount + 1);

        if (eventsSinceLastCheckpoint >= 50)
        {
            await args.UpdateCheckpointAsync();
            partitionEventCount[partition] = 0;
        }
    }
    catch (Exception ex)
    {
        // It is very important that you always guard against
        // exceptions in your handler code; the processor does
        // not have enough understanding of your code to
        // determine the correct action to take.  Any
        // exceptions from your handlers go uncaught by
        // the processor and will NOT be redirected to
        // the error handler.

        logger.LogCritical(ex, "Unexpected failure.");
    }
}

Task processErrorHandler(ProcessErrorEventArgs args)
{
    try
    {
        logger.LogError("Error in the EventProcessorClient. Operation: '{operation}' Exception: {exception}", args.Operation, args.Exception);
    }
    catch (Exception ex)
    {
        // It is very important that you always guard against
        // exceptions in your handler code; the processor does
        // not have enough understanding of your code to
        // determine the correct action to take.  Any
        // exceptions from your handlers go uncaught by
        // the processor and will NOT be handled in any
        // way.

        logger.LogCritical(ex, "Unexpected failure.");
    }

    return Task.CompletedTask;
}
