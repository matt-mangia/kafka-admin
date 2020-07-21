package kafkaadmin.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class RoleBindingResource {
    public String resourceType;
    public String name;
    public String patternType;
    @Override
    public boolean equals(Object obj) {
        if (obj == null) { return false; }
        if (obj == this) { return true; }
        if (obj.getClass() != getClass()) {
            return false;
        }
        RoleBindingResource rhs = (RoleBindingResource) obj;
        return new EqualsBuilder()
                .append(resourceType, rhs.resourceType)
                .append(name, rhs.name)
                .append(patternType, rhs.patternType)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).
                append(resourceType).
                append(name).
                append(patternType).
                toHashCode();
    }
}