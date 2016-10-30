package org.rabix.bindings;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang.NotImplementedException;
import org.rabix.bindings.model.Application;
import org.rabix.bindings.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BindingsFactory {

  private final static Logger logger = LoggerFactory.getLogger(BindingsFactory.class);

  public static Bindings bindingsInUse = null;
  
  private static SortedSet<Bindings> bindings = new TreeSet<>(new Comparator<Bindings>() {
    @Override
    public int compare(Bindings b1, Bindings b2) {
      return b1.getProtocolType().order - b2.getProtocolType().order;
    }
  });

  static {
    for (ProtocolType type : ProtocolType.values()) {
      try {
        Class<?> clazz = Class.forName(type.bindingsClass);
        if (clazz == null) {
          continue;
        }
        try {
          bindings.add((Bindings) clazz.newInstance());
        } catch (Exception e) {
          logger.debug("Failed to find class " + clazz);
        }
      } catch (Exception e) {
        logger.error("Failed to initialize bindings", e);
      }
    }
  }

  public static synchronized Bindings create(String appURL) throws BindingException {
    if (bindingsInUse != null) {
      return bindingsInUse;
    }
    else {
      for (Bindings binding : bindings) {
        try {
          Application app = binding.loadAppObject(appURL);
          if (app == null) {
            continue;
          }
          if (binding.getProtocolType().appVersion != null && app.getVersion() != null) {
            if (binding.getProtocolType().appVersion.equalsIgnoreCase(app.getVersion())) {
              bindingsInUse = binding;
              return binding;
            } else {
              continue;
            }
          }
          bindingsInUse = binding;
          return binding;
        } catch (NotImplementedException e) {
          throw e; // fail if we do not support this kind of deserialization (Schema salad)
        } catch (Exception ignore) {
        }
      }
    }
    throw new BindingException("Cannot find binding for the payload.");
  }

  public static synchronized Bindings create(Job job) throws BindingException {
    return create(job.getApp());
  }
  
  public static synchronized Bindings create(ProtocolType protocol) throws BindingException {
    for(Bindings binding: bindings) {
      if(binding.getProtocolType().equals(protocol)) {
        bindingsInUse = binding;
        return binding;
      }
    }
    throw new BindingException("Cannot find binding for the protocol.");
  }

}
