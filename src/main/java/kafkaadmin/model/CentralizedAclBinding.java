package kafkaadmin.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.util.ArrayList;
import java.util.Map;

public class CentralizedAclBinding {
        public Map<String, String> resourcePattern;
        public Map<String, Map<String,String>> scope;
        public ArrayList<AclRule> aclRules;
        public String toString() {
            return new com.google.gson.Gson().toJson(this);
        }
        @Override
        public boolean equals(Object obj) {
            if (obj == null) { return false; }
            if (obj == this) { return true; }
            if (obj.getClass() != getClass()) {
                return false;
            }
            CentralizedAclBinding rhs = (CentralizedAclBinding) obj;
            return new EqualsBuilder()
                    .append(resourcePattern, rhs.resourcePattern)
                    .append(scope, rhs.scope)
                    .append(aclRules, rhs.aclRules)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).
                    append(resourcePattern).
                    append(scope).
                    append(aclRules).
                    toHashCode();
        }
        public CentralizedAclBinding()
        {
            resourcePattern = null;
            scope = null;
            aclRules = null;
        }
        public CentralizedAclBinding(CentralizedAclBinding copyCentralizedAclBinding)
        {
            // TODO - this is not real copy
            resourcePattern = copyCentralizedAclBinding.resourcePattern;
            scope = copyCentralizedAclBinding.scope;
            aclRules = copyCentralizedAclBinding.aclRules;
        }
}

