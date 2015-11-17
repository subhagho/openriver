package com.wookler.server.common.model;

import com.google.common.base.Preconditions;
import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.DataNotFoundException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Environment registry containing all the maps for the serialization handlers.
 * <p/>
 * Created by Subho on 2/20/2015.
 */
public class SerializerRegistry implements Configurable {

	public static final class Constants {
		public static final String	CONFIG_NODE_ROOT	= "serializers";
		public static final String	CONFIG_NODE_NAME	= "serializer";
		public static final String	CONFIG_ATTR_CLASS	= "class";
		public static final String	CONFIG_ATTR_IMPL	= "handler";
	}

	private HashMap<Class<?>, Serializer<?>>	serializers		= new HashMap<>();
	private ReentrantLock						registryLock	= new ReentrantLock();

	public SerializerRegistry() {
		// Default serializer for byte array. Does nothing.
		Serializer<byte[]> bs = new ByteSerializer();
		serializers.put(byte[].class, bs);

		// Default UTF-8 serializer for Strings.
		Serializer<String> ss = new UTF8StringSerializer();
		serializers.put(String.class, ss);
	}

	public void lock() {
		registryLock.lock();
	}

	public boolean lock(long timeout, TimeUnit unit)
			throws InterruptedException {
		return registryLock.tryLock(timeout, unit);
	}

	public void unlock() {
		registryLock.unlock();
	}

	/**
	 * Register a new serializer for the specified type.
	 *
	 * @param type
	 *            - Class type handled by the serializer.
	 * @param serializer
	 *            - Serializer implementation.
	 * @return - Self.
	 */
	public SerializerRegistry add(Class<?> type, Serializer<?> serializer) {
		Preconditions.checkNotNull(type);
		Preconditions.checkNotNull(serializer);

		serializers.put(type, serializer);
		return this;
	}

	/**
	 * Get the registered Serializer for this type.
	 *
	 * @param type
	 *            - Class type generic.
	 * @return - Registered serializer or NULL if none found.
	 */
	public Serializer<?> get(Class<?> type) {
		Preconditions.checkNotNull(type);

		if (serializers.containsKey(type)) {
			Serializer<?> serializer = serializers.get(type);
			return serializer;
		}
		return null;
	}

	/**
	 * Find a serialization handler for the specified type. This also searches
	 * to check if any serializer registered for an ancestor class can handle
	 * this type.
	 *
	 * @param type
	 *            - Class type.
	 * @return - Registered serializer or NULL if none found.
	 */
	public Serializer<?> find(Class<?> type) {
		Preconditions.checkNotNull(type);
		return find(type, type);
	}

	private Serializer<?> find(Class<?> current, Class<?> type) {
		if (serializers.containsKey(current)) {
			Serializer<?> s = serializers.get(type);
			if (s.accept(type)) {
				return s;
			}
		} else {
			Class<?> parent = current.getSuperclass();
			if (!parent.equals(Object.class)) {
				return find(parent, type);
			}
		}
		return null;
	}

	/**
	 * Configure this instance using the specified parameters.
	 *
	 * @param config
	 *            - Configuration node for this instance.
	 * @throws -
	 *             Configuration Exception
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		Preconditions.checkNotNull(config);
		try {
			if (config.name().compareTo(Constants.CONFIG_NODE_NAME) != 0)
				throw new ConfigurationException(String.format(
						"Invalid configuration node specified. [name=%s]",
						config.name()));

			if (config instanceof ConfigPath) {
				configSerializer(config);
			} else if (config instanceof ConfigValueList) {
				ConfigValueList cvl = (ConfigValueList) config;
				List<ConfigNode> nodes = cvl.values();
				if (nodes != null && !nodes.isEmpty()) {
					for (ConfigNode node : nodes) {
						configSerializer(node);
					}
				}
			} else {
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			}
		} catch (ConfigurationException e) {
			LogUtils.stacktrace(getClass(), e);
			throw e;
		}
	}

	public void configSerializer(ConfigNode node)
			throws ConfigurationException {
		try {
			if (!(node instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						node.getClass().getCanonicalName()));
			ConfigAttributes attrs = ConfigUtils.attributes(node);
			String type = attrs.attribute(Constants.CONFIG_ATTR_CLASS);
			if (StringUtils.isEmpty(type)) {
				throw new ConfigurationException("Missing attribute. [name="
						+ Constants.CONFIG_ATTR_CLASS + "]");
			}
			Class<?> cls = Class.forName(type);
			String stype = attrs.attribute(Constants.CONFIG_ATTR_IMPL);
			if (StringUtils.isEmpty(stype)) {
				throw new ConfigurationException("Missing attribute. [name="
						+ Constants.CONFIG_ATTR_IMPL + "]");
			}
			Class<?> scls = Class.forName(stype);
			Object o = scls.newInstance();
			if (!(o instanceof Serializer))
				throw new ConfigurationException(
						"Invalid Serializer implementation. [class="
								+ scls.getCanonicalName() + "]");
			Serializer<?> serializer = (Serializer<?>) o;
			if (!serializer.accept(cls)) {
				throw new ConfigurationException(String.format(
						"Invalid Serializer being registered. Serializer [%s] does not handle type [%s]",
						scls.getCanonicalName(), cls.getCanonicalName()));
			}
			serializers.put(cls, serializer);
		} catch (DataNotFoundException e) {
			throw new ConfigurationException("Missing attributes.", e);
		} catch (ClassNotFoundException e) {
			throw new ConfigurationException("Invalid attribute.", e);
		} catch (InstantiationException e) {
			throw new ConfigurationException("Invalid Serializer class.", e);
		} catch (IllegalAccessException e) {
			throw new ConfigurationException("Invalid Serializer class.", e);
		}
	}

	/**
	 * Dispose this configured instance.
	 */
	@Override
	public void dispose() {
		serializers.clear();
	}

	private static final SerializerRegistry REGISTRY = new SerializerRegistry();

	/**
	 * Get the handle to the Serializer registry.
	 *
	 * @return - Serializer registry singleton.
	 */
	public static final SerializerRegistry get() {
		return REGISTRY;
	}

	public static final SerializerRegistry init(ConfigNode config)
			throws ConfigurationException {
		REGISTRY.configure(config);

		return REGISTRY;
	}
}
