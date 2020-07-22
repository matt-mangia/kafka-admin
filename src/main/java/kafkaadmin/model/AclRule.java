package kafkaadmin.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class AclRule {
    public String principal;
    public String permissionType;
    public String host;
    public String operation;

    @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }
        AclRule rhs = (AclRule) obj;
        return new EqualsBuilder()
                .append(principal, rhs.principal)
                .append(permissionType, rhs.permissionType)
                .append(operation, rhs.operation)
                .append(host, rhs.host)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).
                append(principal).
                append(permissionType).
                append(operation).
                append(host).
                toHashCode();
    }
}
