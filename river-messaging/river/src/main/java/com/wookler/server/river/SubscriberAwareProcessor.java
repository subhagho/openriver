package com.wookler.server.river;


/**
 * @author Geeta Iyer (geeta dot iyer at outlook.com)
 *
 * @param <M>
 * 
 *            Subscriber aware Processor, that holds the handle to the
 *            associated Subscriber.
 */
public abstract class SubscriberAwareProcessor<M> extends Processor<M> {

    protected Subscriber<M> subscriber;

    /**
     * @return the subscriber
     */
    public Subscriber<M> getSubscriber() {
        return subscriber;
    }

    /**
     * @param subscriber
     *            the subscriber to set
     */
    public void setSubscriber(Subscriber<M> subscriber) {
        this.subscriber = subscriber;
    }

}
