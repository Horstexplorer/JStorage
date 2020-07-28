/*
 *     Copyright 2020 Horstexplorer @ https://www.netbeacon.de
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.netbeacon.jstorage.server.socket.notification.object;

import de.netbeacon.jstorage.server.internal.notificationmanager.objects.DataNotification;
import de.netbeacon.jstorage.server.internal.usermanager.object.User;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This class takes care of handling the notification transport between the NotificationManager and the NotificationSocket
 */
public class NotificationListener {

    private final User user;
    private final HashMap<String, HashSet<String>> requestedNotifications;
    private final BlockingQueue<DataNotification> notificationQueue = new LinkedBlockingQueue<>();

    /**
     * Creates a new instance of this class
     * @param user the user owning this listener
     * @param requestedNotifications wanted notifications
     */
    public NotificationListener(User user, HashMap<String, HashSet<String>> requestedNotifications){
        this.user = user;
        this.requestedNotifications = requestedNotifications;
    }

    /**
     * Used to offer a notification to this listener
     * <br>
     * This will only be queued if it the notification did not come from the same user and the notification comes from a wanted origin
     *
     * @param notification the notification
     * @throws InterruptedException on exception
     */
    public void offerNotification(DataNotification notification) throws InterruptedException {
        if(notification.getOriginUser() != null && notification.getOriginUser() == this.user){
            return;
        }
        if(notification.getContent() == DataNotification.Content.heartbeat){
            notificationQueue.put(notification);
        }else if(notification.getOriginDB() != null && requestedNotifications.containsKey(notification.getOriginDB())) {
            if ((requestedNotifications.get(notification.getOriginDB()).isEmpty()) || (notification.getOriginTable() != null && requestedNotifications.get(notification.getOriginDB()).contains(notification.getOriginTable()))) {
                notificationQueue.put(notification);
            }
        }
    }

    /**
     * Used to get the next notification from the queue.
     * <br>
     * This basically just wraps blockingQueue.take()
     * @return DataNotification
     * @throws InterruptedException on exception
     */
    public DataNotification getNotification() throws InterruptedException {
        return notificationQueue.take();
    }
}
