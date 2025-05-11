package com.uow.W2151443.client;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient; // Import your etcd client
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance; // Import the inner class
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConcertTicketClientApp {
    private static final Logger logger = Logger.getLogger(ConcertTicketClientApp.class.getName());

    private ManagedChannel channel; // Will be initialized after discovery
    private W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub showManagementStub;
    private W2151443ReservationServiceGrpc.W2151443ReservationServiceBlockingStub reservationStub;

    private final W2151443EtcdNameServiceClient etcdClient;
    private final String serviceBasePath = "/services/W2151443ConcertTicketService"; // Must match server

    public ConcertTicketClientApp(String etcdUrl) {
        this.etcdClient = new W2151443EtcdNameServiceClient(etcdUrl);
        // Channel and stubs will be initialized in connectToService()
    }

    private boolean connectToService() {
        logger.info("W2151443 Client: Attempting to discover services from etcd at " + serviceBasePath);
        List<ServiceInstance> availableServers = etcdClient.discoverServiceInstances(serviceBasePath);

        if (availableServers == null || availableServers.isEmpty()) {
            logger.severe("W2151443 Client: No service instances found for '" + serviceBasePath + "' in etcd. Cannot connect.");
            return false;
        }

        // Simple selection strategy: pick one randomly (or first one)
        ServiceInstance serverToConnect = availableServers.get(new Random().nextInt(availableServers.size()));
        logger.info("W2151443 Client: Discovered " + availableServers.size() + " server(s). Connecting to: " + serverToConnect.getIp() + ":" + serverToConnect.getPort());

        this.channel = ManagedChannelBuilder.forAddress(serverToConnect.getIp(), serverToConnect.getPort())
                .usePlaintext()
                .build();
        this.showManagementStub = W2151443ShowManagementServiceGrpc.newBlockingStub(channel);
        this.reservationStub = W2151443ReservationServiceGrpc.newBlockingStub(channel);
        logger.info("W2151443 Client: Successfully created channel and stubs for " + serverToConnect.getIp() + ":" + serverToConnect.getPort());
        return true;
    }

    public void shutdown() throws InterruptedException {
        if (etcdClient != null) {
            etcdClient.shutdownHeartbeat(); // Good practice, though client usually doesn't register/heartbeat
        }
        if (channel != null && !channel.isShutdown()) {
            logger.info("W2151443 Client: Shutting down gRPC channel.");
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        logger.info("W2151443 Client: Shutdown complete.");
    }

    // --- Test methods (testAddShow, testBrowseEvents, etc.) remain identical ---
    // --- They will now use the stubs initialized by connectToService() ---
    public String testAddShow(String showNameSuffix) {
        if (showManagementStub == null) {
            logger.severe("Client not connected to service. Cannot add show.");
            return null;
        }
        logger.info("W2151443 Client: Attempting to add a new show (" + showNameSuffix + ")...");
        String generatedShowId = "show-W2151443-" + UUID.randomUUID().toString().substring(0,8);

        TierDetails vipTier = TierDetails.newBuilder()
                .setTierId("VIP")
                .setTierName("VIP Seating")
                .setPrice(250.00)
                .setTotalStock(20)
                .setCurrentStock(20)
                .build();
        TierDetails regularTier = TierDetails.newBuilder()
                .setTierId("REG")
                .setTierName("Regular Seating")
                .setPrice(120.00)
                .setTotalStock(100)
                .setCurrentStock(100)
                .build();

        ShowInfo newShow = ShowInfo.newBuilder()
                .setShowId(generatedShowId)
                .setShowName("AwesomeFest W2151443 " + showNameSuffix)
                .setShowDate("2025-07-15")
                .setVenue("Grand Arena")
                .putTiers("VIP", vipTier)
                .putTiers("REG", regularTier)
                .setAfterPartyAvailable(true)
                .setAfterPartyTotalStock(10)
                .setAfterPartyCurrentStock(10)
                .setDescription("The best concert of the year by W2151443 artists!")
                .build();

        AddShowRequest request = AddShowRequest.newBuilder().setShowInfo(newShow).build();
        try {
            GenericResponse response = showManagementStub.addShow(request);
            logger.info("W2151443 Client: AddShow Response ("+showNameSuffix+"): Success=" + response.getSuccess() + ", Message=" + response.getMessage());
            return response.getSuccess() ? generatedShowId : null;
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client: AddShow RPC failed ("+showNameSuffix+"): " + e.getStatus().getCode() + " - " + e.getStatus().getDescription());
            return null;
        }
    }

    public void testBrowseEvents() {
        if (reservationStub == null) {
            logger.severe("Client not connected to service. Cannot browse events.");
            return;
        }
        logger.info("W2151443 Client: Attempting to browse events...");
        BrowseEventsRequest request = BrowseEventsRequest.newBuilder().build();
        try {
            BrowseEventsResponse response = reservationStub.browseEvents(request);
            logger.info("W2151443 Client: BrowseEvents found " + response.getShowsCount() + " shows.");
            for (ShowInfo show : response.getShowsList()) {
                logger.info("  Show: " + show.getShowName() + " (ID: " + show.getShowId() + ") on " + show.getShowDate() +
                        ", AfterPartyStock: " + (show.getAfterPartyAvailable() ? show.getAfterPartyCurrentStock() : "N/A"));
                show.getTiersMap().forEach((tierId, tierDetails) -> {
                    logger.info("    Tier: " + tierDetails.getTierName() + " (ID: " + tierId + "), Price: " + tierDetails.getPrice() + ", Stock: " + tierDetails.getCurrentStock());
                });
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client: BrowseEvents RPC failed: {0}", e.getStatus());
        }
    }

    public void testReserveTicket(String showId, String tierId, int quantity, String customerId) {
        if (reservationStub == null) {
            logger.severe("Client not connected to service. Cannot reserve ticket.");
            return;
        }
        if (showId == null || tierId == null) {
            logger.warning("W2151443 Client: Cannot reserve ticket, showId or tierId is null.");
            return;
        }
        logger.info("W2151443 Client ("+customerId+"): Attempting to reserve " + quantity + " tickets for tier " + tierId + " of show " + showId);
        ReserveTicketRequest request = ReserveTicketRequest.newBuilder()
                .setRequestId("clientReq-W2151443-" + UUID.randomUUID().toString())
                .setShowId(showId)
                .addItems(ReservationItem.newBuilder().setTierId(tierId).setQuantity(quantity).build())
                .setCustomerId(customerId)
                .build();
        try {
            ReservationResponse response = reservationStub.reserveTicket(request);
            logger.info("W2151443 Client ("+customerId+"): ReserveTicket Response: Success=" + response.getSuccess() +
                    ", Message=" + response.getMessage() +
                    (response.getSuccess() ? ", ReservationID=" + response.getReservationId() : ", ErrorCode=" + response.getErrorCode()));
            if(response.hasUpdatedShowInfo()){
                response.getUpdatedShowInfo().getTiersMap().forEach((tid, td) -> {
                    if(td.getTierId().equals(tierId)) // Log only the relevant tier for brevity
                        logger.info("    Tier: " + td.getTierName() + ", New Stock: " + td.getCurrentStock());
                });
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client ("+customerId+"): ReserveTicket RPC failed: " + e.getStatus().getCode() + " - " + e.getStatus().getDescription());
        }
    }
    public void testReserveCombo(String showId, String concertTierId, int concertQuantity, int afterPartyQuantity, String customerId) {
        if (reservationStub == null) {
            logger.severe("Client not connected to service. Cannot reserve combo.");
            return;
        }
        if (showId == null || concertTierId == null) {
            logger.warning("W2151443 Client: Cannot reserve combo, showId or concertTierId is null.");
            return;
        }
        logger.info("W2151443 Client ("+customerId+"): Attempting to reserve COMBO: " + concertQuantity + "x " + concertTierId + " and " + afterPartyQuantity + "x AfterParty for show " + showId);
        ReserveComboRequest request = ReserveComboRequest.newBuilder()
                .setRequestId("clientComboReq-W2151443-" + UUID.randomUUID().toString())
                .setShowId(showId)
                .addConcertItems(ReservationItem.newBuilder().setTierId(concertTierId).setQuantity(concertQuantity).build())
                .setAfterPartyQuantity(afterPartyQuantity)
                .setCustomerId(customerId)
                .build();
        try {
            ReservationResponse response = reservationStub.reserveCombo(request);
            logger.info("W2151443 Client ("+customerId+"): ReserveCombo Response: Success=" + response.getSuccess() +
                    ", Message=" + response.getMessage() +
                    (response.getSuccess() ? ", ReservationID=" + response.getReservationId() : ", ErrorCode=" + response.getErrorCode()));
            if(response.hasUpdatedShowInfo()){
                logger.info("    Updated Show Info: AfterPartyStock: " + response.getUpdatedShowInfo().getAfterPartyCurrentStock());
                response.getUpdatedShowInfo().getTiersMap().forEach((tid, td) -> {
                    if(td.getTierId().equals(concertTierId)) // Log only the relevant tier
                        logger.info("    Tier: " + td.getTierName() + ", New Stock: " + td.getCurrentStock());
                });
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client ("+customerId+"): ReserveCombo RPC failed: " + e.getStatus().getCode() + " - " + e.getStatus().getDescription());
        }
    }

    public static void main(String[] args) {
        // etcd URL is now the primary configuration for the client
        String etcdUrl = "http://localhost:2381"; // Default etcd URL
        // This should match HOST_CLIENT_PORT in your run_etcd.sh

        if (args.length > 0) {
            etcdUrl = args[0]; // Allow overriding etcd URL via command line
            logger.info("W2151443 Client: Using etcd URL from command line: " + etcdUrl);
        } else {
            logger.info("W2151443 Client: Using default etcd URL: " + etcdUrl);
        }

        ConcertTicketClientApp client = new ConcertTicketClientApp(etcdUrl);
        String showId1 = null;
        // String showId2 = null; // Removed as it wasn't used reliably after first add

        try {
            // Attempt to connect to a service instance discovered via etcd
            if (!client.connectToService()) {
                logger.severe("W2151443 Client: Failed to connect to any server instance. Exiting.");
                return; // Exit if no server connection can be made
            }

            // If connected, proceed with test cases
            logger.info("\n--- W2151443: Test Case 1: Adding Shows ---");
            showId1 = client.testAddShow("Pop Night Discovery Test");
            // showId2 = client.testAddShow("Rock Fest Discovery Test"); // Can add a second show

            logger.info("\n--- W2151443: Test Case 2: Browse Events ---");
            client.testBrowseEvents();

            if (showId1 != null) {
                logger.info("\n--- W2151443: Test Case 3: Reserving Tickets for Show 1 ("+showId1+") ---");
                client.testReserveTicket(showId1, "VIP", 1, "AliceDS");
                client.testReserveTicket(showId1, "REG", 5, "BobDS");

                logger.info("\n--- W2151443: Test Case 4: Reserving Combo for Show 1 ("+showId1+") ---");
                client.testReserveCombo(showId1, "VIP", 1, 1, "CharlieDS");
                client.testReserveTicket(showId1, "VIP", 20, "DaveDS");
                client.testReserveCombo(showId1, "REG", 2, 10, "EveDS");
            } else {
                logger.warning("W2151443 Client: Show 1 was not added successfully, skipping dependent tests.");
            }

            // Example for testing with another show if added
            // if (showId2 != null) {
            //      logger.info("\n--- W2151443: Test Case 5: Reserving for Show 2 ("+showId2+") ---");
            //      client.testReserveTicket(showId2, "REG", 10, "FrankDS");
            // }

            logger.info("\n--- W2151443: Test Case 6: Browse Events Again (After Reservations) ---");
            client.testBrowseEvents();

        } finally {
            try {
                client.shutdown();
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "W2151443 Client: Shutdown interrupted.", e);
                Thread.currentThread().interrupt();
            }
        }
    }
}
