package org.cloudcmp.tasks.store;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

public class StoreTask extends Task {
	public StoreTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}

	/* For now we don't support Async operation for store tasks */
	public boolean isAsyncSupported() {
		return false;
	}
}
