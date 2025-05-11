package com.uow.W2151443.client;

import com.uow.W2151443.concert.service.*;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient;
import com.uow.W2151443.discovery.W2151443EtcdNameServiceClient.ServiceInstance;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConcertTicketClientApp {
    private static final Logger logger = Logger.getLogger(ConcertTicketClientApp.class.getName());

    private ManagedChannel channel;
    private W2151443ShowManagementServiceGrpc.W2151443ShowManagementServiceBlockingStub showManagementStub;
    private W2151443ReservationServiceGrpc.W2151443ReservationServiceBlockingStub reservationStub;

    private final W2151443EtcdNameServiceClient etcdClient;
    private final String etcdServiceBasePath = "/services/W2151443ConcertTicketService";
    private final String etcdLeaderPath = etcdServiceBasePath + "/_leader"; // Path where leader registers itself

    public ConcertTicketClientApp(String etcdUrl) {
        this.etcdClient = new W2151443EtcdNameServiceClient(etcdUrl);
    }

    private boolean connectToDiscoveredService() {
        logger.info("W2151443 Client: Attempting to discover leader from etcd at path: " + etcdLeaderPath);
        List<ServiceInstance> leaderInstances = etcdClient.discoverServiceInstances(etcdLeaderPath); // Path might be a key

        ServiceInstance serverToConnect = null;

        if (leaderInstances != null && !leaderInstances.isEmpty()) {
            // The _leader path should ideally store a single leader's info directly as the value of the key,
            // or if it's a directory, the first node found there.
            // Assuming discoverServiceInstances can handle if etcdLeaderPath is a direct key or a dir with one entry.
            // If it returns a list, we take the first one.
            serverToConnect = leaderInstances.get(0);
            logger.info("W2151443 Client: Discovered LEADER instance: " + serverToConnect.getId() + " at " +
                    serverToConnect.getIp() + ":" + serverToConnect.getPort());
        } else {
            logger.warning("W2151443 Client: No specific LEADER instance found at " + etcdLeaderPath +
                    ". Attempting to discover any available server instance from " + etcdServiceBasePath);
            List<ServiceInstance> availableServers = etcdClient.discoverServiceInstances(etcdServiceBasePath);

            if (availableServers == null || availableServers.isEmpty()) {
                logger.severe("W2151443 Client: No service instances found at all. Cannot connect.");
                return false;
            }
            // Fallback: pick a random available server if no specific leader is advertised or found
            serverToConnect = availableServers.get(new Random().nextInt(availableServers.size()));
            logger.info("W2151443 Client: Discovered " + availableServers.size() +
                    " general server(s). Connecting to (randomly selected): " + serverToConnect.getId() +
                    " at " + serverToConnect.getIp() + ":" + serverToConnect.getPort());
        }

        if (serverToConnect == null) { // Should not happen if previous blocks execute correctly
            logger.severe("W2151443 Client: Failed to select a server instance. Cannot connect.");
            return false;
        }

        try {
            this.channel = ManagedChannelBuilder.forAddress(serverToConnect.getIp(), serverToConnect.getPort())
                    .usePlaintext()
                    .build();
            this.showManagementStub = W2151443ShowManagementServiceGrpc.newBlockingStub(channel);
            this.reservationStub = W2151443ReservationServiceGrpc.newBlockingStub(channel);
            logger.info("W2151443 Client: Successfully created channel and stubs for " +
                    serverToConnect.getIp() + ":" + serverToConnect.getPort());
            return true;
        } catch (Exception e) {
            logger.log(Level.SEVERE, "W2151443 Client: Failed to create channel/stubs for " +
                    serverToConnect.getIp() + ":" + serverToConnect.getPort(), e);
            return false;
        }
    }

    public void shutdown() {
        if (etcdClient != null) {
            etcdClient.shutdownHeartbeat();
        }
        if (channel != null && !channel.isShutdown()) {
            logger.info("W2151443 Client: Shutting down gRPC channel.");
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.log(Level.WARNING, "Client gRPC channel shutdown interrupted.", e);
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        logger.info("W2151443 Client: Shutdown complete.");
    }

    // --- Test methods (testAddShow, testBrowseEvents, etc.) ---
    // These remain identical to the versions from the previous response where they check for null stubs.
    // I'll include one as an example, the rest are the same.

    public String testAddShow(String showNameSuffix) {
        if (showManagementStub == null) {
            logger.severe("W2151443 Client: Not connected to service. Cannot add show.");
            return null;
        }
        logger.info("W2151443 Client: Attempting to add a new show (" + showNameSuffix + ")...");
        String generatedShowId = "show-W2151443-" + UUID.randomUUID().toString().substring(0,8);

        TierDetails vipTier = TierDetails.newBuilder()
                .setTierId("VIP")
                .setTierName("VIP Seating")
                .setPrice(250.00)
                .setTotalStock(20) // Initial stock
                .setCurrentStock(20)
                .build();
        TierDetails regularTier = TierDetails.newBuilder()
                .setTierId("REG")
                .setTierName("Regular Seating")
                .setPrice(120.00)
                .setTotalStock(100) // Initial stock
                .setCurrentStock(100)
                .build();

        ShowInfo newShow = ShowInfo.newBuilder()
                .setShowId(generatedShowId)
                .setShowName("AwesomeFest W2151443 " + showNameSuffix)
                .setShowDate("2025-08-10") // Changed date slightly for new tests
                .setVenue("Main Arena")
                .putTiers("VIP", vipTier)
                .putTiers("REG", regularTier)
                .setAfterPartyAvailable(true)
                .setAfterPartyTotalStock(10) // Initial stock
                .setAfterPartyCurrentStock(10)
                .setDescription("The ultimate concert event by W2151443 promotions!")
                .build();

        AddShowRequest request = AddShowRequest.newBuilder().setShowInfo(newShow).build();
        try {
            GenericResponse response = showManagementStub.addShow(request);
            logger.info("W2151443 Client: AddShow Response ("+showNameSuffix+"): Success=" + response.getSuccess() + ", Message=" + response.getMessage());
            return response.getSuccess() ? generatedShowId : null;
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client: AddShow RPC failed ("+showNameSuffix+"): " + e.getStatus().getCode() + " - " + e.getStatus().getDescription() + " - Message: " + Status.trailersFromThrowable(e));
            return null;
        }
    }

    public void testBrowseEvents() {
        if (reservationStub == null) {
            logger.severe("W2151443 Client: Not connected to service. Cannot browse events.");
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
            logger.severe("W2151443 Client: Not connected to service. Cannot reserve ticket.");
            return;
        }
        if (showId == null || tierId == null) {
            logger.warning("W2151443 Client: Cannot reserve ticket, showId or tierId is null for customer " + customerId);
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
                    if(td.getTierId().equals(tierId))
                        logger.info("    Tier: " + td.getTierName() + ", New Stock: " + td.getCurrentStock());
                });
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client ("+customerId+"): ReserveTicket RPC failed: " + e.getStatus().getCode() + " - " + e.getStatus().getDescription() + " - Message: " + Status.trailersFromThrowable(e));
        }
    }
    public void testReserveCombo(String showId, String concertTierId, int concertQuantity, int afterPartyQuantity, String customerId) {
        if (reservationStub == null) {
            logger.severe("W2151443 Client: Not connected to service. Cannot reserve combo.");
            return;
        }
        if (showId == null || concertTierId == null) {
            logger.warning("W2151443 Client: Cannot reserve combo, showId or concertTierId is null for customer " + customerId);
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
                    if(td.getTierId().equals(concertTierId))
                        logger.info("    Tier: " + td.getTierName() + ", New Stock: " + td.getCurrentStock());
                });
            }
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "W2151443 Client ("+customerId+"): ReserveCombo RPC failed: " + e.getStatus().getCode() + " - " + e.getStatus().getDescription() + " - Message: " + Status.trailersFromThrowable(e));
        }
    }


    public static void main(String[] args) {
        String etcdUrl = "http://localhost:2381"; // Default etcd URL (where your etcd v3.3.27 is running)
        if (args.length > 0) {
            etcdUrl = args[0]; // Allow overriding etcd URL via command line arg
        }
        logger.info("W2151443 Client: Using etcd URL: " + etcdUrl);

        ConcertTicketClientApp client = new ConcertTicketClientApp(etcdUrl);
        String showIdForTests = null;

        try {
            if (!client.connectToDiscoveredService()) {
                logger.severe("W2151443 Client: CRITICAL - Failed to connect to any server instance via etcd. Exiting.");
                return;
            }

            logger.info("\n--- W2151443 Client: Test Scenario ---");

            // 1. Add a new show
            showIdForTests = client.testAddShow("Grand Gala Night");
            if (showIdForTests == null) {
                logger.severe("W2151443 Client: Failed to add initial show for testing. Exiting further tests.");
                return;
            }

            // 2. Browse events to see the new show
            client.testBrowseEvents();

            // 3. Reserve some tickets for the new show
            client.testReserveTicket(showIdForTests, "VIP", 2, "Alice_VIP_Test"); // Reserve 2 VIP
            client.testReserveTicket(showIdForTests, "REG", 5, "Bob_REG_Test");   // Reserve 5 Regular

            // 4. Reserve a combo
            client.testReserveCombo(showIdForTests, "VIP", 1, 1, "Charlie_Combo_Test"); // 1 VIP + 1 AfterParty

            // 5. Attempt to overbook (these should ideally fail or show reduced stock correctly)
            client.testReserveTicket(showIdForTests, "VIP", 18, "Dave_Overbook_VIP"); // Initial VIP: 20. Used: 2+1=3. Left: 17. Should fail if trying 18.
            client.testReserveCombo(showIdForTests, "REG", 1, 10, "Eve_Overbook_AP"); // Initial AP: 10. Used: 1. Left: 9. Should fail if trying 10.

            // 6. Browse events again to see final stock
            client.testBrowseEvents();

        } catch (Exception e) {
            logger.log(Level.SEVERE, "W2151443 Client: An unexpected error occurred in the main test flow.", e);
        } finally {
            client.shutdown();
        }
    }
}
