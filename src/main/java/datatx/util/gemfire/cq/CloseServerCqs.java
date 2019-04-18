package datatx.util.gemfire.cq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.cache.query.internal.CqQueryImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;

public class CloseServerCqs implements Function, Declarable {
	private static final long serialVersionUID = -4470966767486248329L;
	private final Logger LOG = LogManager.getLogger(CloseServerCqs.class);

	public List<String> closeCqs() {
		List<String> closedCqs = new ArrayList<String>();
		InternalCache ic = (InternalCache) CacheFactory.getAnyInstance();
		CqQuery[] cqs = CacheFactory.getAnyInstance().getQueryService().getCqs();
		for (int i = 0; i < cqs.length; i++) {
			if (cqs[i].isRunning()) {
				closedCqs.add(cqs[i].getName());
			}
		}
		if (closedCqs.size() > 0) {
			CqQueryImpl impl = (CqQueryImpl) ic.getQueryService();
			try {
				impl.close(true);
			} catch (CqClosedException | CqException e) {
				LOG.error("Exception closing CQs - Error: " + e.getMessage(), e);
			}
		}
		return closedCqs;
	}

	@Override
	public void execute(FunctionContext fc) {
		List<String> closedCqs = closeCqs();
		if (closedCqs.size() == 0) {
			closedCqs.add("No CQ queries active");
		}
		fc.getResultSender().lastResult(closedCqs);
	}

	@Override
	public void init(Properties arg0) {
	}

	@Override
	public String getId() {
		return this.getClass().getSimpleName();
	}

	@Override
	public boolean hasResult() {
		return true;
	}

	@Override
	public boolean isHA() {
		return true;
	}

	@Override
	public boolean optimizeForWrite() {
		return false;
	}
}
