exports = async function (orderId, statusId) {
  // Helper function to get status name
  function getStatusName(statusId) {
    const statusNames = {
      1: "Order Received",
      2: "Pending Delivery",
      3: "Delivery In Progress",
      4: "Delivered",
      5: "Cancelled",
      6: "Returned",
    };
    return statusNames[statusId] || "Unknown Status";
  }

  try {
    // Access the default database and collection
    const serviceName = "NoSQL";
    const database = "bookstore";
    const collection = context.services.get(serviceName).db(database).collection("orders");

    // Find the order document
    const order = await collection.findOne({ _id: orderId });

    if (!order) {
      console.log(`Order ${orderId} not found`);
      return { success: false, message: `Order ${orderId} not found` };
    }

    // Get the current status
    let currentStatus = null;
    if (order.history && order.history.length > 0) {
      currentStatus = order.history[order.history.length - 1].status._id;
    }

    // If status is different and valid, update it
    if (currentStatus !== statusId && statusId >= 1 && statusId <= 6) {
      // Get the next history ID
      let nextHistoryId = 1;
      if (order.history && order.history.length > 0) {
        nextHistoryId = Math.max(...order.history.map((h) => h._id)) + 1;
      }

      // Create new status entry
      const newStatus = {
        _id: nextHistoryId,
        status: {
          _id: statusId,
          date: new Date().toISOString().split("T")[0], // YYYY-MM-DD format
          status: getStatusName(statusId),
        },
      };

      // Update the document
      const result = await collection.updateOne(
        { _id: orderId },
        { $push: { history: newStatus } },
      );

      if (result.modifiedCount > 0) {
        console.log(`Status updated successfully for order ${orderId}`);
        return {
          success: true,
          message: `Status updated successfully for order ${orderId}`,
          newStatus: getStatusName(statusId),
        };
      } else {
        console.log(`Failed to update status for order ${orderId}`);
        return {
          success: false,
          message: `Failed to update status for order ${orderId}`,
        };
      }
    } else {
      const message =
        currentStatus === statusId
          ? `Order ${orderId} already has status ${statusId}`
          : `Invalid status ID ${statusId}`;
      console.log(message);
      return { success: false, message: message };
    }
  } catch (error) {
    console.error(`Error updating order status: ${error.message}`);
    return {
      success: false,
      message: `Error updating order status: ${error.message}`,
    };
  }
};
